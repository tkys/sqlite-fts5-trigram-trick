import sqlite3
import time
import pandas as pd 

# データベースに接続
conn = sqlite3.connect('example.db')
c = conn.cursor()

# FTS5トリグラムテーブルを作成
c.execute('''
    CREATE VIRTUAL TABLE IF NOT EXISTS trigram_fts USING fts5(content, tokenize="trigram");
''')

# 仮想語彙テーブルを作成
c.execute('''
    CREATE VIRTUAL TABLE IF NOT EXISTS trigram_fts_vocab USING fts5vocab(trigram_fts, 'row');
''')

# サンプルデータを挿入（実際には、適切なデータを挿入）
#c.execute("INSERT INTO trigram_fts (content) VALUES ('サンプルテキスト')")
# ...


def sanitize_input(input_string):
    """
    サニタイズ関数。
    必要に応じて、特殊文字のエスケープや不正な文字の削除を行います。
    """
    # 例: シングルクォートをエスケープ
    sanitized_string = input_string.replace("'", "''")
    return sanitized_string


def is_no_data():
    # データ有無
    c.execute('''
        SELECT COUNT(*) FROM trigram_fts;
    ''') 
    
    cnt = c.fetchall()[0][0]

    if cnt >= 1:
        return False
    else:
        return True


def migration_db():
    csv = "data.csv"
    df = pd.read_csv(csv, delimiter="\t")

    for text in df["title"]:
        sanitized_text = sanitize_input(text)
        c.execute("INSERT INTO trigram_fts (content) VALUES (?)", (str(sanitized_text),))

    # データベースをコミット
    conn.commit()


def search(query):
    # クエリの長さに応じて検索を行う
    if len(query) < 3:
        # 1文字または2文字の検索
        c.execute(f'''
            SELECT rowid, *
            FROM trigram_fts 
            WHERE trigram_fts
            MATCH (
                SELECT GROUP_CONCAT('"' || term || '"', ' OR ') FROM trigram_fts_vocab WHERE term LIKE '{query}%'
                )
        ''')
    else:
        # 3文字以上の検索
        c.execute(f'''
            SELECT rowid, * 
            FROM trigram_fts 
            WHERE trigram_fts
            MATCH '{query}'
        ''')

    # 検索結果を表示
    result = []
    #for row in c.fetchall():
    #    result.append(row)
             
    return c.fetchall()#result


# データ有無チェック　無ければ投入
if is_no_data() == True:
    migration_db()
else:
    pass    

# 検索クエリを実行
#print(search("検索"))  # 例: "検索"

import time
st= time.time()
search("tri")    # 例: "の"
print(time.time()-st)

# リソースを閉じる
conn.close()
