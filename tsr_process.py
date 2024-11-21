# get_ipython().system('conda env list')

# get_ipython().system('pip install pandas')

# # 必要なライブラリのインストール
# get_ipython().system('pip install tabula-py')
# get_ipython().system('pip install pdfminer.six')
# get_ipython().system('pip install PyPDF2')
# get_ipython().system('pip install mojimoji')
# get_ipython().system('pip install jeraconv')
# get_ipython().system('pip install tqdm')
# get_ipython().system('pip install PyDrive')
# get_ipython().system('pip install joblib')

from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LAParams
from io import StringIO
from tqdm import tqdm
from dateutil.relativedelta import relativedelta
from jeraconv import jeraconv
from PyPDF2 import PdfReader
from tabula.io import read_pdf

import os
import pandas as pd
import tabula
import warnings
import mojimoji
import re
import csv
from datetime import datetime

warnings.simplefilter('ignore')

os.getcwd()

import tabula
print(tabula.__file__)

# read_path = "/Users/akamine_saki/Projects/notebook/TSR"
read_path = os.path.join(os.getcwd(), "TSR")


# フォルダ内の最初のディレクトリを取得
dir_list = os.listdir(read_path)
pdf_dir = dir_list[0]  # 最初のディレクトリ

# フルパスを作成
path = os.path.join(read_path, pdf_dir)

# フォルダ内のファイルやディレクトリを表示
dir_ = os.listdir(path)
dir_  # これが最終的な結果

pattern = '.*.pdf'

files = [f for f in dir_ if re.match(pattern,f)]

def read_pdf_by_row(f, page):
    warnings.simplefilter("ignore")
    columns = ['企業コード(TSR)','上場区分','TSR調査年月日','企業名ﾌﾘｶﾞﾅ（半角）','代表者氏名ﾌﾘｶﾞﾅ（半角）','会社名','代表者氏名','郵便番号','所在地','電話番号','設立年月日','創業年月日','資本金（千円単位）','従業員数','業種1','業種2','業種3',
                   '営業種目','営業所・支店住所','役員','仕入先','株主構成','販売先','財務情報1_時期','財務情報1_売上(千円)','財務情報1_純利益(千円)','財務情報2_時期','財務情報2_売上(千円)','財務情報2_純利益(千円)','財務情報3_時期','財務情報3_売上(千円)','財務情報3_純利益(千円)',
                    '取引銀行','売上伸長率','利益伸長率','TSR備考','代表者住所','生年月日','出身地','出身校']
    
    df_ = pd.DataFrame(columns = columns)
    df_.loc[0] = columns 

    # print
    
    try:
        #df = tabula.read_pdf(f,encoding = 'utf-8',lattice=True, pages=page)
        df = read_pdf(f,encoding = 'utf-8',lattice=True, pages=page)
        #df = read_pdf(f, encoding='cp932', lattice=True, pages=page)
        #print(df)
        value0 = df[0].columns[0] #企業コード　TSRID__c
        value1 = df[1].columns[1][2:] #上場区分 Listing_classification__c
        value2 = df[2].columns[0] #調査年月日
        value3 = mojimoji.zen_to_han(df[3].columns[0]) #商号（カナ）KANA__C
        value4 = mojimoji.zen_to_han(df[4].columns[0]) #代表者カナ　PRESIDENTKANA__C
        value5 = df[5].columns[0] #商号（漢字）NAME
        value6 = df[6].columns[0] #代表者氏名　PRESIDENTNAME__C
        # value6_1  = value6.split(' ')[0] #姓
        # value6_2  = value6.split(' ')[1] #名
        
        value7 = df[7].columns[0].strip('〒')  #郵便番号 BILLINGPOSTALCODE
        value8 = df[8].columns[1] #所在地
        value9 = df[9].columns[0] #電話番号 PHONE
        value10 = df[10].columns[1] #設立年月 STARTDATE__C
        value11 = df[11].columns[1] #創業年月
        value12 = df[12].columns[0] #資本金 CAPITAL__C
        value13 = df[13].columns[0].strip('人') #従業員数赤嶺追加
        # value14 = df[14]  #業種
        value14_1 = df[14].columns[0] #業種1
        value14_2 = df[14].iloc[0][0] #業種2
        value14_3 = df[14].iloc[1][0] #業種3

        value15 = df[15].columns[0].replace('\r','') #営業種目
        value16 = df[16].columns[1].replace('\r','') #営業所・支店
        value17 = df[17].columns[0].replace('\r','') #役員
        value18 = df[18].columns[1].replace('\r','') #仕入先
        value19 = df[19].columns[0].replace('\r','') #大株主
        value20 = df[20].columns[1].replace('\r','') #販売先

        # value21 = df[21] #業績
        value21_1_RECYM = df[21].iloc[0][0] #期間1
        value21_1_AMOUNT = df[21].iloc[0][1] #売上1
        value21_1_PROFIT = df[21].iloc[0][4] #利益1
        value21_2_RECYM = df[21].iloc[1][0] #期間2
        value21_2_AMOUNT = df[21].iloc[1][1] #売上2
        value21_2_PROFIT = df[21].iloc[1][4] #利益2
        value21_3_RECYM = df[21].iloc[2][0] #期間3
        value21_3_AMOUNT = df[21].iloc[2][1] #売上3
        value21_3_PROFIT = df[21].iloc[2][4] #利益3

        value22 = df[22].columns[1].replace('\r','') #取引銀行
        value23 = df[23].columns[1].replace('\r','') #売上伸長率
        value24 = df[24].columns[0] #利益伸長率
        value25 = df[25].columns[1].replace('\r','') #概況
      # value26 = df[26].columns[0].replace('\r','') #代表者
        value27 = df[27].columns[0].replace('\r','') #代表者住所
        value28 = df[28].columns[0].replace('\r','')[:10] #生年月日
      # value29 = df[29].columns[0].replace('\r','') #干支
        value30 = df[30].columns[0].replace('\r','')[2:] #出身地
        value31 = df[31].columns[1].replace('\r','') #出身校

        #値をリスト化
        value = [value0,value1,value2,value3,value4,value5,value6,value7,value8,value9,value10,value11,value12,value13,value14_1,value14_2,value14_3,
                value15,value16,value17,value18,value19,value20,value21_3_RECYM,value21_3_AMOUNT,value21_3_PROFIT,value21_2_RECYM,value21_2_AMOUNT,value21_2_PROFIT,value21_1_RECYM,value21_1_AMOUNT,value21_1_PROFIT,
                value22,value23,value24,value25,value27,value28,value30,value31]
        

        #テーブルを作成
        df = pd.DataFrame([value],columns = columns)
        #テーブルに追加
        #df_ = df_.append(df,ignore_index = True)
        df_ = pd.concat([df_, df], ignore_index=True)
        #print(f'{f}を処理中({startpdf+t}/{endpdf})')
        #CSVへ変換
        #try :
        #   df_.to_csv(f'{f}_csv.csv',encoding='cp932')
        #except :
        #   df_.to_csv(f'{f}_csv.csv',encoding='utf-8')
        return value
    except Exception as e:
        print('表示するページはありません!')
        print(e)


pdf_records = []
for f in files:
    full_path = os.path.join('TSR', '2024', f)
    # full_path = os.path.abspath(f)  # 絶対パスを取得
    
    print(f'{full_path}を読み込み')

    try:
        reader = PdfReader(full_path)
        print(f'ページ数: {len(reader.pages)}')

        start = 1
        # end = min(10, len(reader.pages))
        end = len(reader.pages)+1
        pages =range(start, end)
        
        for page in pages:
            print(f'ページ {page} を処理中')  
            pdf_records.append(read_pdf_by_row(full_path, page))
            
    except FileNotFoundError:
        print(f'ファイルが見つかりません: {full_path}')
    except Exception as e:
        print(f'エラーが発生しました: {e}')

#業種コード一覧を読み込み   
Industry_code = pd.read_excel('TSR業種コード一覧.xlsx')
#業種コードを4桁0埋め
Industry_code['TSR業種コード'] = Industry_code['TSR業種コード'].astype(str).str.zfill(4)
Industry_code['中分類コード'] = Industry_code['中分類コード'].astype(str).str.zfill(2)
Industry_code['小分類コード'] = Industry_code['小分類コード'].astype(str).str.zfill(3)
Industry_code['細分類コード'] = Industry_code['細分類コード'].astype(str).str.zfill(4)
Industry_code = Industry_code.iloc[:,1:]
Industry_code

columns = ['企業コード(TSR)','上場区分','TSR調査年月日','企業名ﾌﾘｶﾞﾅ（半角）','代表者氏名ﾌﾘｶﾞﾅ（半角）','会社名','代表者氏名','郵便番号','所在地','電話番号','設立年月日','創業年月日','資本金（千円単位）','従業員数','業種1','業種2','業種3',
               '営業種目','営業所・支店住所','役員','仕入先','株主構成','販売先','財務情報1_時期','財務情報1_売上(千円)','財務情報1_純利益(千円)','財務情報2_時期','財務情報2_売上(千円)','財務情報2_純利益(千円)','財務情報3_時期','財務情報3_売上(千円)','財務情報3_純利益(千円)',
                '取引銀行','売上伸長率','利益伸長率','TSR備考','代表者住所','生年月日','出身地','出身校']
# df= pd.DataFrame(columns = columns)


#df = pd.read_csv("/Users/akamine_saki/Projects/notebook/output.csv",encoding ="cp932",header=None, names=columns)
df = pd.DataFrame(pdf_records, columns=columns)

#process_1 ％の削除
def StripPercentSign(text: pd.Series) -> pd.Series:
    return text.replace('%', '',)

#process_2 従業員数から「人」を抜く処理
def StripHuman(text: pd.Series) -> pd.Series:
    return text.replace('人', '') 

#process_3 資本金から「千円」を抜く処理
def StriThousandYen(text: pd.Series) -> pd.Series:
    return text.replace('千円', '')

#process_4 全角処理
def ChangeZenkaku(text: pd.Series) -> pd.Series:
    return text.apply(mojimoji.han_to_zen)

#process_5 不要項目の削除
def DeleteEmpty(text: str) -> str:
    return text.replace(['Unnamed: 0','Unnamed: 1','Unnamed: 2','named: 0',"Ｕｎｎａｍｅｄ：　１"],'')

#process_6（株）,(有)の処理（置き換え）
def ChangeStock(text: pd.Series) -> pd.Series:
    text = text.str.replace('(株)', '株式会社')
    text = text.str.replace('(有)', '有限会社')
    text = text.str.replace('(資)', '有限会社')
    text = text.apply(mojimoji.han_to_zen)
    text = text.str.replace('－', '-')
    return text

#process_7 住所分割
def addres_split(addres):
    pattern1 = '東京都|北海道|大阪府|京都府|.{2,3}県'
    pattern2 = """'足立区|荒川区|板橋区|江戸川区|大田区|葛飾区|北区|江東区|品川区|渋谷区|新宿区|杉並区|墨田区|世田谷区|台東区|中央区|千代田区|豊島区|中野区|練馬区|文京区|港区|目黒区|
                .*市.*[^0-9]区|西村山郡|.*?区|市川市|市原市|野々市市|四日市市|廿日市市|.*?市|.*?郡|.*?島'"""
    pattern3 = """.*[- －][0-9 ０-９]{1,4}[^A-Z ^Ａ-Ｚ][A Ａ B Ｂ C Ｃ D Ｄ E Ｅ F Ｆ].*|.*[- －][0-9 ０-９]{1,4}[^A-Z ^Ａ-Ｚ][A Ａ B Ｂ C Ｃ D Ｄ E Ｅ F Ｆ].*|.*[- －][^A-Z ^Ａ-Ｚ][A Ａ B Ｂ C Ｃ D Ｄ E Ｅ F Ｆ].*[0-9 ０-９]{1,4}|.*[- －][^A-Z ^Ａ-Ｚ][A Ａ B Ｂ C Ｃ D Ｄ E Ｅ F Ｆ].*|.*[0-9 ０-９]{1,4}区[0-9 ０-９]{1,4}[- －][0-9 ０-９]{1,4}|.*[0-9 ０-９]{1,4}区[0-9 ０-９]{1,4}|.*[0-9 ０-９]{1,4}番地[0-9 ０-９]{1,4}|[0-9 ０-９]{1,4}区[0-9 ０-９]{1,4}|
                .*[0-9 ０-９]{1,4}地割.*[0-9 ０-９]{1,6}-[0-9 ０-９]{1,6}-[0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,4}地割.*[0-9 ０-９]{1,6}[- －][0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,4}地割.*[0-9 ０-９]{1,4}|
                .*[- －][0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,6}条通り[0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,6}条[0-9 ０-９]{1,6}丁目[0-9 ０-９]{1,6}[- －][0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,6}条[0-9 ０-９]{1,6}丁目|.*[0-9 ０-９]{1,6}画地|.*[- －][0-9 ０-９]{1,6}|.*[0-9 ０-９]{1,6}[a-z A-Z]|.*[0-9 ０-９]{1,6}|[一-龥]+"""
    try :
        todohuken = re.match(pattern1,addres).group()
    except :
        todohuken = ''
    addres1 = addres[len(todohuken):]

    try :
        sikuchoson = re.match(pattern2,addres1).group()
    except :
        sikuchoson = ''
    addres2 = addres1[len(sikuchoson):]

    try :
        banchi = re.match(pattern3,addres2).group()
        if re.match('.*?[a-z ａ-ｚ A-Z Ａ-Ｚ].*[ァ-ヴ]{3,}.*',banchi):#建物名にローマ字とカタカナが含まれている場合（Ａｇｏｒａビルディング）
            banchi = re.sub('[a-z ａ-ｚ A-Z Ａ-Ｚ].*[ァ-ヴ].*','',banchi)
        elif re.search('第[0-9 ０-９ 〇-九].*ビル.*',banchi):#建物名が第～ビルの場合(東興第２ビル２Ｆ)
            match = re.search('第[0-9 ０-９ 〇-九].*ビル.*',banchi).group() 
            banchi = banchi.replace(match,'')
        elif re.search('[a-z ａ-ｚ A-Z Ａ-Ｚ]{1,10}[一-龥々ヶノツ]+[ァ-ヴ]*ビル.*',banchi):#建物名にローマ字と漢字が含まれている場合（ＡＬＤＥＺ紗那）
            match = re.search('[a-z ａ-ｚ A-Z Ａ-Ｚ]{1,10}[一-龥々ヶノツ]+[ァ-ヴ]*ビル.*',banchi).group() 
            banchi = banchi.replace(match,'')
        elif re.search('[一-龥々ヶノツ]+ビル.*',banchi):#建物名が漢字＋ビルの場合
            match = re.search('[一-龥々ヶノツ]+ビル.*',banchi).group() 
            banchi = banchi.replace(match,'')
        elif re.search('[ァ-ヴ - ー]{3,}',banchi):#建物名がカタカナ＋ビルの場合(エスポワールビル)
            match = re.search('[ァ-ヴ - ー].*',banchi).group() 
            banchi = banchi.replace(match,'')
        elif re.search('[a-z ａ-ｚ A-Z Ａ-Ｚ]{2,10}[一-龥々ヶノツ]*.*',banchi):#建物名がローマ字＋数字の場合（SAP４０８）
            match = re.search('[a-z ａ-ｚ A-Z Ａ-Ｚ]{2,10}[一-龥々ヶノツ]*.*',banchi).group()
            banchi = banchi.replace(match,'')
        elif re.match('.*[a-z ａ-ｚ A-Z Ａ-Ｚ][- －][A-Z Ａ-Ｚ]{1,20}',banchi):#建物名にハイフンが含まれている場合（T-Biz）
            banchi = re.sub('[a-z ａ-ｚ A-Z Ａ-Ｚ].*','',banchi)
        elif re.match('.*[a-z ａ-ｚ A-Z Ａ-Ｚ][A-Z Ａ-Ｚ]{1,20}',banchi):#建物名がローマ字の場合(Agora)
            banchi = re.sub('[a-z ａ-ｚ A-Z Ａ-Ｚ].*','',banchi)
        else :
            banchi = banchi
    except :
        banchi = ''
        
    addres3 = addres2[len(banchi):]
    try :
        match = re.match('[- －]',addres3).group()
        addres3 = addres3[len(match):]
    except:
        addres3 = addres3
    tatemonomei = addres3

    return todohuken,sikuchoson,banchi,tatemonomei
    todohuken,sikuchoson,banchi,tatemonomei = addres_split(addres)

#process_8　上場区分を判別するための関数を定義
def listing(list):
    if list == '未上場':
       listing_division = '未上場'
    else:
        listing_division = '上場'
    return listing_division

df['上場/未上場'] = df['上場区分'].apply(lambda x : listing(x))

#process_9 名前分割
def name_split(name):
      pattern = ' |　|・'
      try :
        sei = re.split(pattern,name)[0]
        mei = re.split(pattern,name)[1]
      except f:
        sei = ''
        mei = name
        sei,mei = name_split(name)
      return sei,mei
      # sei,mei = name_split(name)



#業種1～3に分類
df['業種1No'] = df['業種1'].str[:5].astype(str).str.zfill(4)
df['業種1'] = df['業種1'].str[5:]
df['業種2No'] = df['業種2'].str[:5]
df['業種2'] = df['業種2'].str[5:]
df['業種3No'] = df['業種3'].str[:5]
df['業種3'] = df['業種3'].str[5:]


#株主一覧をリスト化
holders_list = df['株主構成'].to_list()
holder1_name_list = []
holder1_percent_list = []
holder2_name_list = []
holder2_percent_list = []
holder3_name_list = []
holder3_percent_list = []

for i in range(len(holders_list)):
    try :
        holder1 = re.split('[,，]',holders_list[i])[0] #正規表現で、, で分割している
        holder1_name = re.sub('\(.+?\)|\（.+?\）','',holder1)
        holder1_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder1)
    except :
        holder1 = ''
        holder1_name = ''
        holder1_percent = ''
    try :
        holder2 = re.split('[,，]',holders_list[i])[1]
        holder2_name = re.sub('\(.+?\)|\（.+?\）','',holder2)
        holder2_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder2)
    except :
        holder2 = ''
        holder2_name = ''
        holder2_percent = ''
    try :
        holder3 = re.split('[,，]',holders_list[i])[2]
        holder3_name = re.sub('\(.+?\)|\（.+?\）','',holder3) 
        holder3_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder3)
    except :
        holder3 = ''
        holder3_name = ''
        holder3_percent = ''

    holder1_name_list.append(holder1_name)
    holder1_percent_list.append(holder1_percent)

    holder2_name_list.append(holder2_name)
    holder2_percent_list.append(holder2_percent)

    holder3_name_list.append(holder3_name)
    holder3_percent_list.append(holder3_percent)

#リスト内の保有割合を囲んでいる特殊文字[]を削除
holder1_percent_lists = []
holder2_percent_lists = []
holder3_percent_lists = []
for i in range(len(df)):
    try :
        holder1_percent = holder1_percent_list[i][0]
    except :
        holder1_percent = ''

    try :
        holder2_percent = holder2_percent_list[i][0]
    except :
        holder2_percent = ''

    try :
        holder3_percent = holder3_percent_list[i][0]
    except :
        holder3_percent = ''

    holder1_percent_lists.append(holder1_percent)
    holder2_percent_lists.append(holder2_percent)
    holder3_percent_lists.append(holder3_percent)

#株主項目にリストの中身を追加
df['第一位株主名称'] = holder1_name_list
df['第一位株主_株式保有割合'] = holder1_percent_lists
df['第二位株主名称'] = holder2_name_list
df['第二位株主_株式保有割合'] = holder2_percent_lists
df['第三位株主名称'] = holder3_name_list
df['第三位株主_株式保有割合'] = holder3_percent_lists

#代表者、株主一致チェック
holder_check_list = []
for i in range(0,len(df)):
    representative_ = df['代表者氏名'][i]
    representative = representative_.replace('　','')
    target = '　'
    idx = representative_.find(target)
    representative_first_name = representative[:idx]
    first_holder = df['第一位株主名称'][i]
    holders_ = df['株主構成'][i]
    holders = re.sub('\(.+?\)','',holders_)
    if representative == first_holder:
        check = '◎'
    elif re.findall(representative,holders):
        check = '〇'
    elif representative_first_name in holders:
        check = '△'
    else :
        check = '×'
    holder_check_list.append(check)
df['株主・代表取締役一致'] = holder_check_list

#process_1
df["売上伸長率"] = df["売上伸長率"].apply(StripPercentSign)
df["利益伸長率"] = df["利益伸長率"].apply(StripPercentSign)

#process_2
df["従業員数"] = df["従業員数"].apply(StripHuman)

#process_3
df["資本金（千円単位）"] = df["資本金（千円単位）"].apply(StriThousandYen)

#process_4
df["営業所・支店住所"] = ChangeZenkaku(df["営業所・支店住所"])
df["仕入先"] = ChangeZenkaku(df["仕入先"])
df["販売先"] = ChangeZenkaku(df["販売先"])
df["株主構成"] = ChangeZenkaku(df["株主構成"])
df["所在地"] = ChangeZenkaku(df["所在地"])
df['第一位株主_株式保有割合'] = ChangeZenkaku(df["第一位株主_株式保有割合"])
df['第二位株主_株式保有割合'] = ChangeZenkaku(df["第二位株主_株式保有割合"])
df['第三位株主_株式保有割合'] = ChangeZenkaku(df["第三位株主_株式保有割合"])

#process_5
df = df.apply(DeleteEmpty)

#process_6
df["会社名"] = ChangeStock(df["会社名"])

#process_7 所在地を都道府県、市区町村、町名・番地、建物名に分割
df['都道府県'] = df['所在地'].apply(lambda x:addres_split(x)[0])
df['市区町村'] = df['所在地'].apply(lambda x:addres_split(x)[1])
df['町名・番地'] = df['所在地'].apply(lambda x:addres_split(x)[2])
df['町名・番地'] = df['町名・番地'].str.replace('－','-')
df['建物名'] = df['所在地'].apply(lambda x:addres_split(x)[3])
df['建物名'] = df['建物名'].str.replace('－','-')

#process_7 
df['代表者氏名'] = df['代表者氏名'].str.replace(' ','　')
df['代表者氏名(姓)'] = df['代表者氏名'].apply(lambda x:name_split(x)[0])
df['代表者氏名(名)'] = df['代表者氏名'].apply(lambda x:name_split(x)[1])
df['財務情報1_時期'] = pd.to_datetime(df['財務情報1_時期'],format='%Y/%m')
df['財務情報1_時期'] = df['財務情報1_時期'] + pd.to_timedelta(df['財務情報1_時期'].dt.days_in_month,'d') - pd.to_timedelta('1 days')


df['第一位株主_株式保有割合'] = df['第一位株主_株式保有割合'].astype(str)
df['第二位株主_株式保有割合'] = df['第二位株主_株式保有割合'].astype(str)
df['第三位株主_株式保有割合'] = df['第三位株主_株式保有割合'].astype(str)

#株主一覧をリスト化
holders_list = df['株主構成'].to_list()
holder1_name_list = []
holder1_percent_list = []
holder2_name_list = []
holder2_percent_list = []
holder3_name_list = []
holder3_percent_list = []

for i in range(len(holders_list)):
    try :
        holder1 = re.split('[,，]',holders_list[i])[0]
        holder1_name = re.sub('\(.+?\)|\（.+?\）','',holder1)
        holder1_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder1)
    except :
        holder1 = ''
        holder1_name = ''
        holder1_percent = ''
    try :
        holder2 = re.split('[,，]',holders_list[i])[1]
        holder2_name = re.sub('\(.+?\)|\（.+?\）','',holder2)
        holder2_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder2)
    except :
        holder2 = ''
        holder2_name = ''
        holder2_percent = ''
    try :
        holder3 = re.split('[,，]',holders_list[i])[2]
        holder3_name = re.sub('\(.+?\)|\（.+?\）','',holder3) 
        holder3_percent = re.findall("(?<=\().+?(?=\))|(?<=\（).+?(?=\）)", holder3)
    except :
        holder3 = ''
        holder3_name = ''
        holder3_percent = ''

    holder1_name_list.append(holder1_name)
    holder1_percent_list.append(holder1_percent)

    holder2_name_list.append(holder2_name)
    holder2_percent_list.append(holder2_percent)

    holder3_name_list.append(holder3_name)
    holder3_percent_list.append(holder3_percent)

#リスト内の保有割合を囲んでいる特殊文字[]を削除
holder1_percent_lists = []
holder2_percent_lists = []
holder3_percent_lists = []
for i in range(len(df)):
    try :
        holder1_percent = holder1_percent_list[i][0]
    except :
        holder1_percent = ''

    try :
        holder2_percent = holder2_percent_list[i][0]
    except :
        holder2_percent = ''

    try :
        holder3_percent = holder3_percent_list[i][0]
    except :
        holder3_percent = ''

    holder1_percent_lists.append(holder1_percent)
    holder2_percent_lists.append(holder2_percent)
    holder3_percent_lists.append(holder3_percent)

#株主項目にリストの中身を追加
df['第一位株主名称'] = holder1_name_list
df['第一位株主_株式保有割合'] = holder1_percent_lists
df['第二位株主名称'] = holder2_name_list
df['第二位株主_株式保有割合'] = holder2_percent_lists
df['第三位株主名称'] = holder3_name_list
df['第三位株主_株式保有割合'] = holder3_percent_lists

df['第一位株主_株式保有割合'] = df['第一位株主_株式保有割合'].astype(str)
df['第二位株主_株式保有割合'] = df['第二位株主_株式保有割合'].astype(str)
df['第三位株主_株式保有割合'] = df['第三位株主_株式保有割合'].astype(str)

#代表者、株主一致チェック
holder_check_list = []
for i in range(0,len(df)):
    representative_ = df['代表者氏名'][i]
    representative = representative_.replace('　','')
    target = '　'
    idx = representative_.find(target)
    representative_first_name = representative[:idx]
    first_holder = df['第一位株主名称'][i]
    holders_ = df['株主構成'][i]
    holders = re.sub('\(.+?\)','',holders_)
    if representative == first_holder:
        check = '◎'
    elif re.findall(representative,holders):
        check = '〇'
    elif representative_first_name in holders:
        check = '△'
    else :
        check = '×'
    holder_check_list.append(check)
df['株主・代表取締役一致'] = holder_check_list

#設立年月を日付型に変換にするための関数を定義
j2w = jeraconv.J2W()
list_ = []
list = df['設立年月日'].to_list()
#元号(漢字表記)を西暦に変換
for i in range(len(list)):
    try :
        gengo = j2w.convert(list[i])
    except :
        gengo = list[i]
    list_.append(str(gengo))

#年のみの場合、1月に指定
list2_ = []
for i in range(len(list_)):
    if re.match('^[0-9]{4}$',list_[i]):
        gengo = f'{list_[i]}/01/01'
    elif re.match('[0-9]{4}/[0-9]{2}',list_[i]):
        gengo = f'{list_[i]}/01'
    elif re.match(u'[一-龥]+',list_[i]):
        gengo = ''
    else :
        gengo = list[i]
    list2_.append(gengo)
array = pd.Series(data=list2_)
df['設立年月日'] = array 

#年月を日付型に変換にするための関数を定義
j2w = jeraconv.J2W()
list_ = []
list = df['創業年月日'].to_list()
#元号(漢字表記)を西暦に変換
for i in range(len(list)):
    try :
        gengo = j2w.convert(list[i])
    except :
        gengo = list[i]
    list_.append(str(gengo))

#年のみの場合、1月に指定
list2_ = []
for i in range(len(list_)):
    if re.match('^[0-9]{4}$',list_[i]):
        gengo = f'{list_[i]}/01/01'
    elif re.match('[0-9]{4}/[0-9]{2}',list_[i]):
        gengo = f'{list_[i]}/01'
    elif re.match(u'[一-龥]+',list_[i]):
        gengo = ''
    else :
        gengo = list[i]
    list2_.append(gengo)
array = pd.Series(data=list2_)
df['創業年月日'] = array 


#設立、創業年月を日付型に変換するための関数を定義
def to_timestamp(gengo):
    try:
        time = dt.strptime(gengo, '%Y/%m/%d')
    except ValueError:
        time = gengo  # 日付の形式が合わない場合はそのまま返す
    return time

 #代表者の生年月日を日付型にするための関数を定義
import re
def birthday(date):
    if date == '' :
        date = ''
    elif re.match('[0-9 ０-９]{4}[/ ／][0-9 ０-９]{2}[/ ／][0-9 ０-９]{2}[/ ／]',date):
        date = date
    elif re.match('[0-9 ０-９]{4}[ |　]',date):
        date = f'{date[:4]}/01/01'
    elif re.match('[0-9 ０-９]{4}[/ ／][0-9 ０-９]{2}[ |　]',date):
        date = f'{date[:7]}/01'
    return date
df['生年月日'] = df['生年月日'].apply(lambda x:birthday(x))

#年月を日付型に変換
try :
  df['設立年月日'] = df['設立年月日'].apply(lambda x:to_timestamp(x))
except :
  df['設立年月日'] = df['設立年月日']
try :
  df['創業年月日'] = df['創業年月日'].apply(lambda x:to_timestamp(x))
except :
  df['創業年月日'] = df['創業年月日']

try :
  df['財務情報1_時期'] = pd.to_datetime(df['財務情報1_時期'],format='%Y/%m')
  df['財務情報1_時期'] = df['財務情報1_時期'] + pd.to_timedelta(df['財務情報1_時期'].dt.days_in_month,'d') - pd.to_timedelta('1 days')
except :
  df['財務情報1_時期'] = df['財務情報1_時期']


try :
  df['財務情報2_時期'] = pd.to_datetime(df['財務情報2_時期'],format='%Y/%m')
  df['財務情報2_時期'] = df['財務情報2_時期'] + pd.to_timedelta(df['財務情報2_時期'].dt.days_in_month,'d') - pd.to_timedelta('1 days')
except :
  df['財務情報2_時期'] = df['財務情報2_時期']

try :
  df['財務情報3_時期'] = pd.to_datetime(df['財務情報3_時期'],format='%Y/%m')
  df['財務情報3_時期'] = df['財務情報3_時期'] + pd.to_timedelta(df['財務情報3_時期'].dt.days_in_month,'d') - pd.to_timedelta('1 days')
except :
  df['財務情報3_時期'] = df['財務情報3_時期']

#業種Noから業種分類を特定　空白処理
df['業種1No'] = df['業種1No'].str.strip()
df['業種2No'] = df['業種2No'].str.strip()
df['業種3No'] = df['業種3No'].str.strip()


#業種コード一覧をマージ
df_merge1 = pd.merge(df,Industry_code,left_on = '業種1No',right_on = '細分類コード',how = 'left')
df_merge2 = pd.merge(df_merge1,Industry_code,left_on = '業種2No',right_on = '細分類コード',how = 'left')
df_merge3 = pd.merge(df_merge2,Industry_code,left_on = '業種3No',right_on = '細分類コード',how = 'left')
df_merge3.iloc[:,56:]
df_merge3.columns
df_merge3 = df_merge3.rename(columns = {'大分類_x':'大分類1','中分類コード_x':'中分類コード1','中分類_x':'中分類1','小分類コード_x':'小分類コード1','小分類_x':'小分類1','細分類コード_x':'細分類コード1', '細分類_x':'細分類1',
       '大分類_y':'大分類2','中分類コード_y':'中分類コード2', '中分類_y':'中分類2', '小分類コード_y':'小分類コード2', '小分類_y':'小分類2','細分類コード_y':'細分類コード2', '細分類_y':'細分類2', 
       '大分類':'大分類3','中分類コード':'中分類コード3', '中分類':'中分類3', '小分類コード':'小分類コード3', '小分類':'小分類3','細分類コード':'細分類コード3', '細分類':'細分類3'})
df_merge3.to_excel('TSRデータ_業種一覧追加ver.xlsx')
