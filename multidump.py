# coding: utf-8
# todo: dump dbs from 2 locations by putty-ssh-pgdump to local drive
# parse dumps to two folders (host-dbname) by files (filename: key(expression)
# run folders compare
from urlparse import urlparse
import subprocess
import os
import re
import shutil
import platform
import config

class ExecuteRemoteException(Exception):
    pass

def execute(dsn, command, env):
    parsed = urlparse(dsn)
    exenv = os.environ
    if parsed.scheme == 'file':
        exenv.update(exenv)
    else:
        command = map(lambda pair: "%s=%s" % pair, env.iteritems()) + command
        command = platform.remote(parsed) + [
            parsed.hostname,
            subprocess.list2cmdline(command),
        ]

    #print command
    p = subprocess.Popen(command, stderr = subprocess.PIPE, stdout = subprocess.PIPE, env = exenv)
    out = p.communicate()
    if out[1]:
        err = unicode(out[1], 'cp1251').encode('cp866')
        #raise ExecuteRemoteException("process wrote to stderr:\n\n%s" % err + "\n\n" + " ".join(command))
    if p.returncode:
        err = unicode(out[1], 'cp1251').encode('cp866')
        raise ExecuteRemoteException('process exited with status: %s\n%s\n' % p.returncode, err, )
    return out[0]

def dump(dsn):
    parsed = urlparse(dsn)
    parsed = urlparse('http:/' + parsed.path)
    cmd = [
        'pg_dump',
        '-i',
        '--format=plain',
        '--schema-only',
        '--no-privileges',
        '--host=' + parsed.hostname,
        '--username=' + parsed.username,
    ]
    if parsed.port:
        cmd += ('--port=' + parsed.port,)
    cmd += (parsed.path[1:],)
    env = {'PGPASSWORD': parsed.password}
    
    cmd = filter(len, cmd)
    return execute(dsn, cmd, env)

def put(content, path):
    dir = 'dump/' + os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    f = file('dump/' + path, 'wb+')
    f.write(content)
    f.close()

def split(dump):
    MODE_HEADER_PRE = 'H<'
    MODE_HEADER_POST = 'H>'
    MODE_BODY = 'B*'
    
    mode = MODE_HEADER_PRE
    
    header_keys = ('name', 'type', 'schema', 'owner', )
    re_header = '; '.join(map(lambda k: k.title() + ': (?P<' + k + '>[^;]*)', header_keys))
    re_header = '^-- ' + re_header + '.*$'
    re_header = re.compile(re_header)
    
    re_body_ddblock = re.compile('\$([0-9a-zA-Z_]*)\$')
    
    # @todo???
    re_ignored_line = re.compile('^SET (?:search_path|default_with_oids) = [^;]+;$')
    #re_ignored_header_name = re.compile(r'^_slonycluster_') 
    re_ignored_header_schema = re.compile(r'^'+config.IGNORE_SHEMAS+'$')
    re_comment_inline = re.compile(r'(.*)--.*')
    re_comment_block  = re.compile(r'(.*)/\*.*?\*/(.*)')
    re_comment_block_open = re.compile(r'(.*)/\*.*')
    re_comment_block_close = re.compile(r'.*?\*/(.*)')
    
    body = None
    header = None
    result = []
    ddblock_stack = []
    ignore_block = False
    comment = False
    
    for line in dump.splitlines():
        print " %s %s %d <%s>" % (mode, int(comment), len(ddblock_stack), line, )
        if mode == MODE_HEADER_PRE:
            if line == '--':
                continue
            elif line == '-- PostgreSQL database dump':
                mode = MODE_HEADER_POST
                header = {'name': 'init', 'type': 'init', 'schema': 'init', }
                continue
            elif line == '-- PostgreSQL database dump complete':
                return result
            else:
                r = re_header.match(line)
                if r:
                    header = {}
                    for k in header_keys:
                        header[k] = r.group(k)

                    if re_ignored_header_schema.search(header['schema']): # or re_ignored_header_name.search(header['name'])
                        ignore_block = True
                    mode = MODE_HEADER_POST
                    continue
                else:
                    raise Exception('parse bug: case 2')
        elif mode == MODE_HEADER_POST:
            if line == '--':
                continue
            elif line == '':
                mode = MODE_BODY
                body = []
                continue
            else:
                raise Exception('parse bug: case 3')
        elif mode == MODE_BODY:
            srcline = line
            if comment:
                if re_comment_block_close.match(line):
                    line = re.sub(re_comment_block_close, r'\1', line)
                    comment = False
                else:
                    continue
            
            line = re.sub(re_comment_block, r'\1 \2', line)
            line = re.sub(re_comment_inline, r'\1', line)
            if re_comment_block_open.match(line):
                line = re.sub(re_comment_block_open, r'\1', line)
                comment = True
            
            for r in re_body_ddblock.finditer(line):
                name = r.group(1)
                if len(ddblock_stack)>0 and ddblock_stack[-1] == name:
                    ddblock_stack.pop()
                    print 'stack >> "%s"' %name
                else:
                    ddblock_stack.append(name)
                    print 'stack << "%s"' %name
            if len(ddblock_stack) > 0:
                body.append(line)
                #print 'stack items: %d' % len(ddblock_stack) 
                continue
            elif re_ignored_line.match(line):
                continue
            elif srcline == '--':
                if not ignore_block:
                    result.append((
                        header, body,
                    ))
                header, body, ddblock_stack = None, None, [],
                ignore_block = False
                mode = MODE_HEADER_PRE
                continue
            else:
                body.append(line)
                continue
        break
    raise Exception('parse bug: case 1')

def pgdef2filename(name):
    return name\
        .replace(r'@', r'_at_')\
        .replace(r'[]', r'_()_')\
        .replace(r'#', r'_hash_')\
        .replace(r'&&', r'_damp_')\
        .replace(r'&', r'_amp_')\
        .replace(r'<', r'_lt_')\
        .replace(r'>', r'_gt_')\
        .replace(r'|', r'_pipe_')\
        .replace(r'"', r'_quot_')[0:150]

def distribute(name, result):
    if os.path.exists('dump/' + name):
        shutil.rmtree('dump/' + name, False)
    for h, b in result:
        #print 'name: ' + h.get('name',   'unknown')
        path = '%s/%s %s.%s.sql' % (
            name,
            h.get('type',   'unknown'),
            h.get('schema', 'unknown'),
            pgdef2filename(h.get('name',   'unknown')),
        )
        put("\n" . join(b), path)

def make(dsn):
    gate = urlparse(dsn)
    db = urlparse('sftp:/' + gate.path)
    name = " - ".join((gate.hostname if gate.hostname else 'localhost', db.hostname, db.path[1:], ))
    distribute(name, split(dump(dsn)))
    return name

def run(dsn1, dsn2):
    p1 = 'dump/' + make(dsn1)
    p2 = 'dump/' + make(dsn2)
    
    os.system(subprocess.list2cmdline(
        platform.DIFF_TOOL +
        [
            p1,
            p2,
        ]
    ))
