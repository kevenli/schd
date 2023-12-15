import sys
from schd.scheduler import main as scheduler_main

def main():
    sys.path.append('.')
    scheduler_main()


if __name__ == '__main__':
    main()
