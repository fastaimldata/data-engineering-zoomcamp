import sys
import argparse
import pandas as pd

DAYS_OF_WEEK = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

def main(args):
    
    if args.day is None:
         print(f'No input provided. Day of the week expected')
    else:
        input_day = args.day.lower()
        
        if input_day in DAYS_OF_WEEK:
            day_no = DAYS_OF_WEEK.index(input_day) + 1
            print(f'You chose day {day_no} of the week!')
        else:
            print(f'Unexpected input {input_day}. Day of the week expected')
    
    print(f'pipeline.py ran successfully for input = {args.day}')

    print('###################### pipeline.py finished ######################')

if __name__ == '__main__':
    
    print('###################### pipeline.py execution ######################')

    parser = argparse.ArgumentParser("Pass a sample argument")
    parser.add_argument('--day', type=str, help='Pass day of the week')
    args = parser.parse_args()
    main(args)