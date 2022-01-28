from decouple import Config

DEBUG = True

postgres = {'DB_USER':Config('postgres'),
              'BD_PASSWORD':Config('1234'),
              'BD_HOST':Config('localhost'),
              'DB_PORT ':Config('localhost'),
              'DB_NAME':Config('ALKEMY')
             }

URL = 'postgresql://postgres:1234@localhost:5432/ALKEMY'


