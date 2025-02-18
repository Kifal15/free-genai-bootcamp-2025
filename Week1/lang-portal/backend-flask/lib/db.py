import sqlite3

import json 
from flask import g

class Db:
    def __init__(self, database="word.db"):
        self.database = database 
        self.connection - None 
    def get(self):
        if 'db' not in g:
            g.db =sqlite3.connect(self.database)
            g.db.row_factory = sqlite3.Row
            return g.db
    
    def commit(self):
        self.get().commit()
    
    def cursor(self):
        connection = self.get( )
        return connection.cursor()
    
    def close(self):
        db = g.pop('db', None)

        if db is not None:
            db.close()

    def sql(self , filepath):
        with open('sql/'+filepath,'r') as file: 
            return file.read()
    def load_json(self,filepath):
        with open(filepath , 'r' ) as file:
            return json.load(file)
    
    def setup_tables(self, cursor):
        cursor.execute(self.sql('setup/create_table_words.sql'))
        cursor.execute(self.sql('setup/create_table_word_reviews.sql'))
        cursor.execute(self.sql('setup/create_table_word_review_items.sql'))
        cursor.execute(self.sql('setup/create_table_groups.sql'))
        cursor.execute(self.sql('setup/create_table_word_groups.sql'))
        cursor.execute(self.sql('setup/create_table_study_activities.sql'))
        cursor.execute(self.sql('setup/create_table_study_sessions.sql'))
        self.get().commit()
    
    def import_study_activities_json(self , cursor, data_json_path):
        study_activites =self.load_json(data_json_path)
        for activity in study_activites:
            cursor.execute(''' INSERT INTO study_activities (name , url , preview_url ) VALUES (?,?,?) ''' ,(activity['name'],activity['url'] ,   activity['preview_url']  ) )

        self.get().commit()

    def import_word_json (self , cursor,group_name ,data_json_path):
        cursor.execute(''' INSERT INTO groups (name)  VALUES (?) ''' , (group_name,))
        self.get().commit()

        cursor.execute('SELECT id FROM groups WHERE name = ? ',(group_name,))
        core_verbs_group_id = cursor.fetchone()[0]

        words = self.load_json(data_json_path)

        for word in words:
            cursor.excute(''' INSERT INTO words (urdu , roman , english , parts )  VALUES (?,?,?,?)
            )''' , (word['urdu'] , word['roman'] , word['english'] , json.dumps(word['parts'])))

        word_id = cursor.lastrowid

        cursor.execute(''' INSERT INTO word_groups (word_id, group_id )  VALUES (?,?)''' 
        ,(word_id , core_verbs_group_id) )

        self.get().commit()


        cursor.execute  (''' UPDATE groups SET words_count = (
            SELECT COUNT (*) FROM word_groups WHERE group_id = ? ) 
            WHERE id = ? 
        ''', (core_verbs_group_id , core_verbs_group_id))

        self.get.commit()

        print(f"sucessfully added {len(words)} verbs to the '{group_name}' group.")

        def init ( self , app):
            with app.app_context():
                cursor = self.cursor()
                self.setup_tables(cursor)
                self.import_word_json(
                    cursor = cursor,
                    group_name='Core Verbs',
                    data_json_path='seed/data_verbs.json'
                )
                self.import_word_json(
                    cursor=cursor,
                    group_name='Core Adjectives',
                    data_json_path='seed/data_adjectives.json'
                )

                self.import_study_activities_json(
                    cursor=cursor,
                    data_json_path='seed/study_activities.json'
                 )

db =Db()
