import tkinter
class ThumbHistApp():
    def __init__(self):
        wd = tkinter.Tk()
        wd.title(u'Landsat数据挑选')
        txt = tkinter.Entry(wd, width=10).pack()
        wd.mainloop()

if __name__ == '__main__':
    app = ThumbHistApp()