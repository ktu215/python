from api.Kiwoom import *
from util.make_up_universe import *
from util.db_helper import *
from util.time_helper import *
# from util.notifier import *
import math
import traceback


class RSIStrategy(QThread):
    def __init__(self):
        QThread.__init__(self)
        self.strategy_name = "RSIStrategy"
        self.kiwoom = Kiwoom()

        # 유니버스 정보를 담을 딕셔너리
        self.universe = {}

        # 계좌 예수금
        self.deposit = 0

        # 초기화 함수 성공 여부 확인 변수
        self.is_init_success = False

        self.init_strategy()

    def init_strategy(self):
        """전략 초기화 기능을 수행하는 함수"""
        try:
            # 유니버스 조회, 없으면 생성
            self.check_and_get_universe()

            # 가격 정보를 조회, 필요하면 생성
            self.check_and_get_price_data()

            # Kiwoom > 주문정보 확인
            self.kiwoom.get_order()

            # Kiwoom > 잔고 확인
            self.kiwoom.get_balance()

            # Kiwoom > 예수금 확인
            self.deposit = self.kiwoom.get_deposit()

            # 유니버스 실시간 체결정보 등록
            self.set_universe_real_time()

            self.is_init_success = True

        except Exception as e:
            print(traceback.format_exc())
            # LINE 메시지를 보내는 부분
            send_message(traceback.format_exc(), RSI_STRATEGY_MESSAGE_TOKEN)

    def check_and_get_universe(self):
        """유니버스가 존재하는지 확인하고 없으면 생성하는 함수"""
        if not check_table_exist(self.strategy_name, 'universe'):
            universe_list = get_universe()
            print(universe_list)
            universe = {}
            # 오늘 날짜를 20210101 형태로 지정
            now = datetime.now().strftime("%Y%m%d")

            # KOSPI(0)에 상장된 모든 종목 코드를 가져와 kospi_code_list에 저장
            kospi_code_list = self.kiwoom.get_code_list_by_market("0")

            # KOSDAQ(10)에 상장된 모든 종목 코드를 가져와 kosdaq_code_list에 저장
            kosdaq_code_list = self.kiwoom.get_code_list_by_market("10")

            for code in kospi_code_list + kosdaq_code_list:
                # 모든 종목 코드를 바탕으로 반복문 수행
                code_name = self.kiwoom.get_master_code_name(code)

                # 얻어온 종목명이 유니버스에 포함되어 있다면 딕셔너리에 추가
                if code_name in universe_list:
                    universe[code] = code_name

            # 코드, 종목명, 생성일자자를 열로 가지는 DaaFrame 생성
            universe_df = pd.DataFrame({
                'code': universe.keys(),
                'code_name': universe.values(),
                'created_at': [now] * len(universe.keys())
            })

            # universe라는 테이블명으로 Dataframe을 DB에 저장함
            insert_df_to_db(self.strategy_name, 'universe', universe_df)

        sql = "select * from universe"
        cur = execute_sql(self.strategy_name, sql)
        universe_list = cur.fetchall()
        for item in universe_list:
            idx, code, code_name, created_at = item
            self.universe[code] = {
                'code_name': code_name
            }
        print(self.universe)

    def check_and_get_price_data(self):
        """일봉 데이터가 존재하는지 확인하고 없다면 생성하는 함수"""
        for idx, code in enumerate(self.universe.keys()):
            print("({}/{}) {}".format(idx + 1, len(self.universe), code))

            # (1)케이스: 일봉 데이터가 아예 없는지 확인(장 종료 이후)
            if check_transaction_closed() and not check_table_exist(self.strategy_name, code):
                # API를 이용해 조회한 가격 데이터 price_df에 저장
                price_df = self.kiwoom.get_price_data(code)
                # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                insert_df_to_db(self.strategy_name, code, price_df)
            else:
                # (2), (3), (4) 케이스: 일봉 데이터가 있는 경우
                # (2)케이스: 장이 종료된 경우 API를 이용해 얻어온 데이터를 저장
                if check_transaction_closed():
                    # 저장된 데이터의 가장 최근 일자를 조회
                    sql = "select max(`{}`) from `{}`".format('index', code)

                    cur = execute_sql(self.strategy_name, sql)

                    # 일봉 데이터를 저장한 가장 최근 일자를 조회
                    last_date = cur.fetchone()

                    # 오늘 날짜를 20210101 형태로 지정
                    now = datetime.now().strftime("%Y%m%d")

                    # 최근 저장 일자가 오늘이 아닌지 확인
                    if last_date[0] != now:
                        price_df = self.kiwoom.get_price_data(code)
                        # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                        insert_df_to_db(self.strategy_name, code, price_df)

                # (3), (4) 케이스: 장 시작 전이거나 장 중인 경우 데이터베이스에 저장된 데이터 조회
                else:
                    sql = "select * from `{}`".format(code)
                    cur = execute_sql(self.strategy_name, sql)
                    cols = [column[0] for column in cur.description]

                    # 데이터베이스에서 조회한 데이터를 DataFrame으로 변환해서 저장
                    price_df = pd.DataFrame.from_records(data=cur.fetchall(), columns=cols)
                    price_df = price_df.set_index('index')
                    # 가격 데이터를 self.universe에서 접근할 수 있도록 저장
                    self.universe[code]['price_df'] = price_df

    def run(self):
        while self.is_init_success:
            try:
                # (0)장중인지 확인
                if not check_transaction_open():
                    print("장시간이 아니므로 5분간 대기합니다.")
                    time.sleep(5 * 60)
                    continue

                for idx, code in enumerate(self.universe.keys()):
                    print('[{}/{}_{}]'.format(idx + 1, len(self.universe), self.universe[code]['code_name']))
                    time.sleep(0.5)

                    if code not in self.kiwoom.universe_realtime_transaction_info.keys():
                        # print(self.kiwoom.universe_realtime_transaction_info[code])
                        print('없음')

                    # (1)접수한 주문이 있는지 확인
                    if code in self.kiwoom.order.keys():
                        # (2)주문이 있음
                        print('접수 주문', self.kiwoom.order[code])

                        # (2.1) '미체결수량' 확인하여 미체결 종목인지 확인
                        if self.kiwoom.order[code]['미체결수량'] > 0:
                            pass

                    # (3)보유 종목인지 확인
                    elif code in self.kiwoom.balance.keys():
                        print('보유 종목', self.kiwoom.balance[code])
                        # (6)매도 대상 확인
                        if self.check_sell_signal(code):
                            # (7)매도 대상이면 매도 주문 접수
                            pass

            except Exception as e:
                print(traceback.format_exc())
                # LINE 메시지를 보내는 부분
                # send_message(traceback.format_exc(), RSI_STRATEGY_MESSAGE_TOKEN)

    def set_universe_real_time(self):
        """유니버스 실시간 체결정보 수신 등록하는 함수"""
        # 임의의 fid를 하나 전달하기 위한 코드(아무 값의 fid라도 하나 이상 전달해야 정보를 얻어올 수 있음)
        fids = get_fid("체결시간")

        # 장운영구분을 확인하고 싶으면 사용할 코드
        # self.kiwoom.set_real_reg("1000", "", get_fid("장운영구분"), "0")

        # universe 딕셔너리의 key값들은 종목코드들을 의미
        codes = self.universe.keys()

        # 종목코드들을 ';'을 기준으로 묶어주는 작업
        codes = ";".join(map(str, codes))
        print(codes)

        # 화면번호 9999에 종목코드들의 실시간 체결정보 수신을 요청
        # self.kiwoom.set_real_reg("9999", codes, fids, "0")


    def check_sell_signal(self, code):
        """매도대상인지 확인하는 함수"""
        universe_item = self.universe[code]
        print(universe_item)
        print(universe_item.keys())

        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매도대상 확인 과정에서 아직 체결정보가 없습니다")