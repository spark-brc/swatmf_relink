import time 
from colorama import init
from colorama import Fore, Style
import os
import pandas as pd
import glob
import subprocess
init()

print('')
print(Fore.CYAN + '              # SWAT-MODFLOW Relinking Process #\n')

class swatmf_relink:
    def __init__(self) -> None:
        pass

    # Print iterations progress
    def printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 20, fill = '#', printEnd = "\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + ' ' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)

        # Print New Line on Complete
        if iteration == total:
            suffix = Fore.GREEN + 'passed'
            print(Fore.RESET + f"\r{prefix} |{bar}| {percent}% ... {suffix}", end = printEnd)
            print()
        else:
            # suffix = Fore.RED + '... processing'
            print(Fore.RESET + f"\r{prefix} |{bar}| {percent}% ... {suffix}", end = printEnd)

    def read_new_subs(self):
        try:
            # search for hru original or hru trimmed
            print(Style.RESET_ALL)
            for i in range(3):
                dots = '.' * i
                print(" Searching for HAWQS's file {}".format(dots),  end='\r', flush=True)
                time.sleep(1)
            if os.path.exists('../hrus-trimmed.csv'):
                f = dots + Fore.GREEN + " 'hrus-trimmed.csv' file found"
                print(" Searching for HAWQS's file {}".format(f))
                df = pd.read_csv('../hrus-trimmed.csv', header=0)
            elif os.path.exists('../hrus-original.csv'):
                f = dots + Fore.GREEN + " 'hrus-original.csv' file found"
                print(" Searching for HAWQS's file {}".format(f))
                df = pd.read_csv('../hrus-original.csv', header=0)
            self.sub_ids = df.Subbasin.unique()
        except Exception as e:
                f = dots + Fore.RED + " FAIL"
                print(" Searching for HAWQS's file {}".format(f))
                e = "   HAWQS's file NOT Found!"
                print(e)


    def create_new_hru_dhru(self):
        sub_ids = self.sub_ids
        sub_ids.sort()
        df = pd.read_csv('../backup/hru_dhru', skiprows=2, header=0, sep=r'\s+')
        df_new = pd.DataFrame()
        prefix = '  - create new hru_dhru  '
        l = len(sub_ids)
        self.printProgressBar(0, l, prefix = prefix)
        for p, i in enumerate(sub_ids):
            df_new = pd.concat([df_new, df[df['subbasin'] == i]], ignore_index=True)
            self.printProgressBar(p + 1, l, prefix = prefix)
        df_new = df_new.sort_values(by=['hru_id', 'dhru_id'])
        df_new['new_did'] = df_new.groupby('dhru_id').ngroup()
        df_new['new_did'] = [i+1 for i in df_new.new_did]
        df_new['new_hid'] = df_new.groupby('hru_id').ngroup()
        df_new['new_hid'] = [i+1 for i in df_new['new_hid']]
        df_new['new_sid'] = df_new.groupby('subbasin').ngroup()
        df_new['new_sid'] = [i+1 for i in df_new['new_sid']]
        self.new_hd = df_new

    def create_new_dhru_grid(self):
        print(Style.RESET_ALL + '  - create new dhru_grid: ')
        prefix1 = '     . extract dhru info '
        df_new_ids = self.new_hd 
        dhru_ids = df_new_ids.dhru_id.unique()
        df = pd.read_csv('../backup/dhru_grid', skiprows=2, header=0, sep=r'\s+')
        df_filtered_dg = pd.DataFrame()
        l1 = len(dhru_ids)
        self.printProgressBar(
            0, l1, prefix = prefix1) 
        for p1, i in enumerate(dhru_ids):
            df_filtered_dg = pd.concat([df_filtered_dg, df[df['dhru_id'] == i]], ignore_index=True)
            self.printProgressBar(
                p1 + 1, l1, prefix = prefix1)
        prefix2 = '     . remap dhru ids    '
        new_dhru_ids = []
        l = len(df_filtered_dg.dhru_id)
        # Initial call to print 0% progress
        self.printProgressBar(
            0, l, prefix = prefix2) 
        for p, i in enumerate(df_filtered_dg.dhru_id):
            new_dhru_id = df_new_ids.loc[df_new_ids['dhru_id'] == i, 'new_did'].iloc[0]
            new_dhru_ids.append(new_dhru_id)
            self.printProgressBar(
                p + 1, l, prefix = prefix2)
        df_filtered_dg['new_did'] = new_dhru_ids
        df_filtered_dg.sort_values(by=['grid_id', 'new_did'], inplace=True)
        self.new_dg = df_filtered_dg

    def create_new_river_grid(self):
        sub_ids = self.sub_ids
        sub_ids.sort()
        l = len(sub_ids)
        prefix = '  - create new river_grid'
        self.printProgressBar(
            0, l, prefix = prefix)
        df = pd.read_csv('../backup/river_grid', skiprows=1, header=0, sep=r'\s+')
        df_new = pd.DataFrame()
        for p, i in enumerate(sub_ids):
            df_new = pd.concat([df_new, df[df['subbasin'] == i]], ignore_index=True)
            self.printProgressBar(
                p + 1, l, prefix = prefix)        
        df_new['new_sid'] = df_new.groupby('subbasin').ngroup()
        df_new['new_sid'] = [i+1 for i in df_new['new_sid']]
        self.new_rg = df_new

    def create_new_fp(self):
        sub_ids = self.sub_ids
        sub_ids.sort()
        l = len(sub_ids)
        prefix = '  - create new fp.dat    '
        self.printProgressBar(
            0, l, prefix = prefix)
        df = pd.read_csv('../backup/fp.dat', names=['subbasin', 'val'], sep=r'\s+')
        df_new = pd.DataFrame()
        for p, i in enumerate(sub_ids):
            df_new = pd.concat([df_new, df[df['subbasin'] == i]], ignore_index=True)
            self.printProgressBar(
                p + 1, l, prefix = prefix)        
        df_new['new_sid'] = df_new.groupby('subbasin').ngroup()
        df_new['new_sid'] = [i+1 for i in df_new['new_sid']]
        self.new_fp = df_new

    def get_grid_num(self):
        for filename in glob.glob("*.dis"):
            with open(filename, "r") as f:
                data = []
                for line in f.readlines():
                    if not line.startswith("#"):
                        data.append(line.replace('\n', '').split())
            nrow = int(data[0][1])
            ncol = int(data[0][2])
        return nrow, ncol

    def print_hru_dhru(self):
        df = self.new_hd
        with open('hru_dhru', 'w') as f:
            f.write(str(len(df.new_did.unique())) + '\n')
            f.write(str(max(df.new_hid)) + '\n')
            f.write('dhru_id    dhru_area   hru_id  subbasin    hru_area' + '\n')
            df[
                ['new_did', 'dhru_area', 'new_hid', 'new_sid', 'hru_area']
                ].to_csv(f, sep = '\t', index=False, header=False, line_terminator='\n', encoding='utf-8')

    def print_dhru_grid(self):
        df = self.new_dg
        nrow, ncol = self.get_grid_num()
        number_of_grids = nrow * ncol

        with open('dhru_grid', 'w') as f:
            f.write(str(len(df)) + '\n')
            f.write(str(number_of_grids) + '\n')
            f.write('grid_id    grid_area   dhru_id  overlap_area    dhru_area' + '\n')
            df[
                ['grid_id', 'grid_area', 'new_did', 'overlap_area', 'dhru_area']
                ].to_csv(f, sep = '\t', index=False, header=False, line_terminator='\n', encoding='utf-8')

    def print_grid_dhru(self):
        df = self.new_dg
        df.sort_values(by=['new_did', 'grid_id'], inplace=True)
        dhru_num = len(self.new_hd.new_did.unique())
        nrow, ncol = self.get_grid_num()
        with open('grid_dhru', 'w') as f:
            f.write(str(len(df)) + '\n')
            f.write(str(dhru_num) + '\n')
            f.write(str(nrow) + '\n')
            f.write(str(ncol) + '\n')
            f.write('grid_id    grid_area   dhru_id  overlap_area    dhru_area' + '\n')
            df[
                ['grid_id', 'grid_area', 'new_did', 'overlap_area', 'dhru_area']
                ].to_csv(f, sep = '\t', index=False, header=False, line_terminator='\n', encoding='utf-8')

    def print_river_grid(self):
        df = self.new_rg
        df.sort_values(by=['grid_id'], inplace=True)
        with open('river_grid', 'w') as f:
            f.write(str(len(df)) + '\n')
            f.write('grid_id    subbasin    rgrid_len' + '\n')
            df[
                ['grid_id', 'new_sid', 'rgrid_len']
                ].to_csv(f, sep = '\t', index=False, header=False, line_terminator='\n', encoding='utf-8')

    def print_fp(self):
        df = self.new_fp
        with open('fp.dat', 'w') as f:
            df[
                ['new_sid', 'val']
                ].to_csv(f, sep = '\t', index=False, header=False, line_terminator='\n', encoding='utf-8')

    def execute_creatswatmf(self):
        print(Style.RESET_ALL)
        for i in range(3, 0, -1):
            print("     We are going to execute 'CreateSWATMF.exe' in {}s!".format(i),  end='\r', flush=True)
            time.sleep(1)
        print()
        print("     Execute!")
        exe_file = "CreateSWATMF.exe"
        p = subprocess.Popen(exe_file) # cwd -> current working directory    
        p.wait()

if __name__ == "__main__":
    # wd = "D:/Projects/Tools/linking_filter/data/new-corb-swat-modflow-test/files/Watershed/text/HAWQS/TxtInOut"
    # os.chdir(wd)
    # print('')
    # print(Fore.CYAN + '              # SWAT-MODFLOW Relinking Process #\n')
    m1 = swatmf_relink()
    m1.read_new_subs()
    m1.create_new_hru_dhru()
    m1.print_hru_dhru()
    m1.create_new_dhru_grid()
    m1.create_new_river_grid()
    m1.print_dhru_grid()
    m1.print_grid_dhru()
    m1.print_river_grid()
    if os.path.exists('../backup/fp.dat'):
        m1.create_new_fp()
        m1.print_fp()
    m1.execute_creatswatmf()
