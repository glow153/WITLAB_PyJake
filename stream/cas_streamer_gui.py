import sys

from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import (QApplication, QDesktopWidget, QLabel, QHBoxLayout,
                             QWidget, QMainWindow, QLineEdit, QPushButton, QAction)


class CasStreamerFrame(QMainWindow):
    def __init__(self, title):
        super(CasStreamerFrame, self).__init__()
        self.title = title
        self.setupUi()
        self.createActions()

        self.is_streaming = False

    def setupUi(self):
        self.setGeometry(0, 0, 520, 100)
        self.setWindowTitle(self.title)
        self.setWindowIcon(QIcon('icon.png'))
        self.wnd2Center()

        # create widgets
        self.layout = QHBoxLayout()
        self.lbl = QLabel('스트리밍 대상 디렉토리')
        self.ledt = QLineEdit()
        self.btn = QPushButton('시작')

        # set layout and stretch widgets
        self.layout.setContentsMargins(5, 5, 5, 5)
        self.layout.setStretchFactor(self.lbl, 2)
        self.layout.setStretchFactor(self.ledt, 7)
        self.layout.setStretchFactor(self.btn, 1)

        # add widgets
        self.layout.addWidget(self.lbl)
        self.layout.addWidget(self.ledt)
        self.layout.addWidget(self.btn)

        self.widget = QWidget()
        self.widget.setLayout(self.layout)
        self.setCentralWidget(self.widget)

    def createActions(self):
        self.btn.clicked.connect(self.toggle_streaming)

    def toggle_streaming(self):
        if self.is_streaming:
            pass
        else:
            pass

        # toggle flag
        self.is_streaming = not self.is_streaming

    def wnd2Center(self):
        # geometry of the main window
        qr = self.frameGeometry()
        # center point of screen
        cp = QDesktopWidget().availableGeometry().center()
        # move rectangle's center point to screen's center point
        qr.moveCenter(cp)
        # top left of rectangle becomes top left of window centering it
        self.move(qr.topLeft())


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CasStreamerFrame('WitLab CAS data streamer v1.0 - jake')
    window.show()
    app.exec_()
