#!/usr/bin/env python
# coding: utf-8
from config import DSN
from multidump import run

from Tkinter import *
class App:
    def __init__(self, master):
        frame = Frame(master, borderwidth = 10)
        frame.pack()
        
        self.dsn_value = StringVar()
        self.dsn_value.set(None)
        for key in DSN.iterkeys():
            Radiobutton(
                frame, 
                text=key, 
                variable=self.dsn_value, 
                value=key
            ).pack(anchor=W)

        self.compare = Button(frame, text="Compare", command=self.compare)
        self.compare.pack(side=BOTTOM)

    def compare(self):
        key = self.dsn_value.get()
        run(*DSN[key])

root = Tk()
app = App(root)
root.mainloop()
