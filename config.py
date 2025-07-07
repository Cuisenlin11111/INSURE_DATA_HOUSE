import configparser
import os
# def load_config(path=r'E:\pycharm\config.ini'):
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

    config = configparser.ConfigParser()
    config.read(config_path)

    # config = configparser.ConfigParser()
    # config.read(path)
    return {
        'host': config.get('CREDENTIALS', 'host'),
        'port': config.getint('CREDENTIALS', 'port'),
        'user': config.get('CREDENTIALS', 'username'),
        'password': config.get('CREDENTIALS', 'password')
    }



# if __name__ == '__main__':
#     print(load_config())