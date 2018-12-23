import logging
from PyQt4 import QtGui,QtTest
from smpq import Manager
from smpq.testing import *
logging.getLogger('smpq').setLevel(logging.DEBUG)

def test_map_sleep_qt(qtbot):
    try:
        w=QtGui.QWidget()
        w.setWindowTitle('I should be responsive the whole time')
        hbox=QtGui.QHBoxLayout()
        w.setLayout(hbox)
        b=QtGui.QPushButton('Press me!')
        b.clicked.connect(lambda:print('See, I responded.'))
        hbox.addWidget(b)
        w.show()
        ##
        with Manager(2,sleep=lambda seconds:QtTest.QTest.qWait(seconds*1000)) as manager:
            results=list(manager.map(square_and_add_slow,range(4)))
    finally:
        w.close()
    
if __name__=="__main__":
    test_map_sleep_qt(None)
