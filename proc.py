'''proc.py - selective process kill prog'''
import psutil
import sys
def main():
    '''Process kill function'''
    for proc in psutil.process_iter():
        # check whether the process name matches
        print(proc.name())
        if any(procstr in proc.name() for procstr in \
               ['python']):
            print(f'Gracefully Killing {proc.name()}')
            proc.kill()
if __name__ == "__main__":
    if sys.argv[1] == 'stop':
        print(sys.argv[0])
        main()
