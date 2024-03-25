from api.Kiwoom import *
from strategy.RSIStrategy import*
import sys

app = QApplication(sys.argv)
kiwoom = Kiwoom()

# fids = get_fid("체결시간")
# codes = '000270;'
# kiwoom.set_real_reg("1000",codes,fids,"0")


rsi_strategy = RSIStrategy()
rsi_strategy.start()


app.exec_()



