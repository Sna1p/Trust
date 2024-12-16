from warnings import filterwarnings
filterwarnings('ignore', category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')
import pandas as pd
from sqlalchemy import create_engine
import fdb

# Создаём класс для работы с БД
class DBWorker():
    # Подключение к БД
    # и получение данных 
    def get_db_med(self, dbname, user, password, query):
        #print('========INIT========')
        self.engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@&&/{dbname}').connect()
        self.df = pd.read_sql(query, con=self.engine)
        return self.df
        #print('========INIT END========')

    def get_db_form(self, dbname, query):
        #print('========INIT========')
        #self.engine = create_engine(db_uri, echo=True)
        self.con = fdb.connect(host='10.0.0.1', port=3050, database=f'g:/&&/&&/&&/&&/{dbname}.gdb', user='SYSDBA', password='masterkey', charset='UTF8', fb_library_name='c:/&&/&&/Desktop/&&/&&.dll')
        self.df = pd.read_sql(query, con=self.con)

        #print(self.df)
        return self.df

    # переписываем даннык из DF
    # в масссив  
    def get_fild(self, df, N, fild: str):
        self.arr = list(range(0,N))
        for i in range(0, N):
            self.arr[i] = (df[fild].loc[df.index[i]])
        return self.arr

    # Отдельно для ФИО так как в БД другой формат записи
    # [Иванов Иван Иванович] --> [[Иванов],[Иван],[Иванович]] 

    def FIO(self, df, N):
        FIOarr = list(range(0, N))
        for i in range(0, N):
            FIOarr[i] = (df['name'].loc[df.index[i]])
        Splitarr = []
        i = 0
        while i <= N-1:
            Splitarr.append(str.split(FIOarr[i]))
            i = i + 1
        return Splitarr

    def Control_in_army(self, df_f, df_m, N):
        Control_arr = []
        for i in range(0, N):
            Found_in_form = df_f[(df_f['IM'] == df_m[i][1]) & (df_f['FAM'] == df_m[i][0])]
            if len(Found_in_form) == 1:
                Control_arr.append(Found_in_form.values.tolist())
        return Control_arr

        

# запрос к БД
query_med = """
  
    """

query_form = """
   
"""

# Функция для формирования подключения и запроса от класса
def DBQuery_med(dbname):
    df = DBWorker.get_db_med(DBWorker, dbname, 'med', 'med', query_med)
    Name = DBWorker.FIO(DBWorker, df, len(df))
    return Name

def DBQuery_form(dbname):
    df = DBWorker.get_db_form(DBWorker, dbname, query_form)
    return df

bd_priz = input("Введите базу ")

resp_med = DBQuery_med(bd_priz)
resp_form = DBQuery_form(f'FORM{bd_priz}')

df = DBWorker.Control_in_army(DBWorker, resp_form, resp_med, len(resp_med))

print("\n Пофомильный списрк контрольников убывших в войска (силы) \n")
for i in range(0, len(df)):
    temp_df = df[i]
    print(f'{temp_df[0][0]} {temp_df[0][1]} {temp_df[0][2]} | Убыл в РА')    
    print('-----------------------------------------------------------')   
 
print(f"\n Всего убыло {len(df)} \n")
input("Для завершения работы введтье Enter ")