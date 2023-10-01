#######################
# cesm2_extvar.py
#######################
# Written by Cartier P. (lcartierp@gmail.com)
# Last updated on 2023/10/02
#######################

import os
import subprocess
import multiprocessing as mp

# Define user input parameters
expid = 'cesm2.1_co2IRCC'
expname = 'co2_IRCC'
casename = 'b.e21.B2000_BPRP'
resolution = 'f09_g17'
ens_id_list = ['0101', '0401', '0701', '1001']
Loverwrite = True
archive_root = '/$FOLDER4/$SUB-FOLDER'
output_root = '/$FOLDER3/$SUB-FOLDER/' + expid
comp_list = ['ocn']
freq_list = ['monthly']
start_year = 2000
end_year = 2400
extract_type = 'extra'  # either 'basic' or 'extra'

# Define dictionaries for variables
extra_vars = {
    'atm': {'monthly': ['T'], 'daily': ['TREFHT'], 'six_hourly': ['TREFHT']},
    'ocn': {'monthly': ['SiO3'], 'daily': []},
    'ice': {'monthly': [], 'daily': []},
    'lnd': {'monthly': [], 'daily': []},
    'rof': {'monthly': [], 'daily': []}
}

# Default settings
model = {'atm': 'cam', 'ocn': 'pop', 'lnd': 'clm2', 'ice': 'cice', 'rof': 'mosart'}
freq_name = {
    'atm': {'monthly': 'h0', 'daily': 'h1', 'six_hourly': 'h2'},
    'ocn': {'monthly': 'h', 'daily': 'h.nday1'},
    'lnd': {'monthly': 'h0', 'daily': 'h3'},
    'ice': {'monthly': 'h', 'daily': 'h1'},
    'rof': {'monthly': 'h0', 'daily': 'h1'}
}

# Define basic variables
basic_vars = {
    'atm': {
        'monthly': ['CLDHGH', 'CLDLOW', 'CLDMED', 'CLDTOT', 'CLOUD', 'CO2', 'CO2_FFF', 'CO2_LND', 'CO2_OCN', 'CONCLD',
                    'FLDS', 'FLDSC', 'FLNR', 'FLNS', 'FLNSC', 'FLNT', 'FLNTC', 'FLUT', 'FLUTC', 'FSDS', 'FSDSC', 'FSNR',
                    'FSNS', 'FSNSC', 'FSNT', 'FSNTC', 'FSNTOA', 'FSNTOAC', 'FSUTOA', 'ICEFRAC', 'LHFLX', 'LWCF', 'OMEGA',
                    'PBLH', 'PRECC', 'PRECL', 'PRECT', 'PS', 'PSL', 'Q', 'QFLX', 'QREFHT', 'RELHUM', 'SFCO2', 'SFCO2_FFF',
                    'SFCO2_LND', 'SFCO2_OCN', 'SHFLX', 'SST', 'SWCF', 'T', 'TAUX', 'TAUY', 'TMQ', 'TREFHT', 'TREFHTMN',
                    'TREFHTMX', 'TS', 'U', 'U10', 'V', 'Z3'],
        'daily': ['CAPE', 'FLDS', 'FLDSC', 'FLNS', 'FLNSC', 'FLNT', 'FLNTC', 'FLUT', 'FLUTC', 'FSDS', 'FSDSC', 'FSNS',
                  'FSNSC', 'FSNTOA', 'FSNTOAC', 'LHFLX', 'OMEGA', 'PBLH', 'PRECC', 'PRECT', 'PS', 'PSL', 'Q', 'QREFHT',
                  'RHREFHT', 'SHFLX', 'T', 'TMQ', 'TREFHT', 'TREFHTMN', 'TREFHTMX', 'TS', 'TSMN', 'TSMX', 'U', 'U10', 'V',
                  'WSPDSRFAV', 'Z3'],
        'six_hourly': ['T850', 'Q850', 'U850', 'V850', 'Z200', 'Z500', 'Z850', 'PSL', 'TREFHT', 'PS', 'PRECT']
    },
    'ocn': {
        'monthly': ['ALK', 'DIC', 'DpCO2', 'FW', 'Fe', 'HMXL', 'HMXL_DR', 'MOC', 'NO3', 'N_HEAT', 'N_SALT', 'O2',
                    'PAR_avg', 'PD', 'PO4', 'POC_FLUX_100m', 'PV', 'Q', 'RHO', 'SALT', 'SSH', 'SSS', 'SST', 'TAUX',
                    'TAUY', 'TBLT', 'TEMP', 'TMXL', 'UVEL', 'VISOP', 'VVEL', 'WVEL', 'XBLT', 'XMXL', 'diatChl',
                    'diazChl', 'pCO2SURF', 'spChl'],
        'daily': ['SST'],
        'eco_daily': ['DpCO2_2', 'spChl_SURF']
    },
    'lnd': {
        'monthly': ['ALT', 'AR', 'BAF_CROP', 'BAF_PEATF', 'COL_FIRE_CLOSS', 'EFLX_LH_TOT', 'ELAI', 'ER', 'ESAI',
                    'FAREA_BURNED', 'FPSN', 'FSA', 'FSDS', 'FSH', 'FSR', 'GPP', 'H2OSOI', 'HR', 'LAISHA', 'LAISUN',
                    'NBP', 'NEE', 'NPP', 'PBOT', 'PCO2', 'PFT_FIRE_CLOSS', 'QH2OSFC', 'QSOIL', 'QVEGT', 'RAIN', 'RH2M',
                    'SOIL1C', 'SOIL1N', 'SOIL2C', 'SOIL2N', 'SOIL3C', 'SOIL3N', 'SOILICE', 'SOILLIQ', 'SOMC_FIRE',
                    'TBOT', 'TG', 'TLAI', 'TOTCOLC', 'TOTLITC', 'TOTSOILICE', 'TOTSOILLIQ', 'TOTSOMC', 'TOTVEGC',
                    'TREFMNAV', 'TREFMXAV', 'TSA', 'TSAI', 'TSOI', 'TSOI_10CM', 'TV', 'WIND', 'ZBOT'],
        'daily': []
    },
    'ice': {
        'monthly': ['aice', 'hi', 'hs'],
        'daily': []
    },
    'rof': {
        'monthly': ['DIRECT_DISCHARGE_TO_OCEAN_ICE', 'DIRECT_DISCHARGE_TO_OCEAN_LIQ',
                    'RIVER_DISCHARGE_OVER_LAND_ICE', 'RIVER_DISCHARGE_OVER_LAND_LIQ',
                    'TOTAL_DISCHARGE_TO_OCEAN_ICE', 'TOTAL_DISCHARGE_TO_OCEAN_LIQ'],
        'daily': []
    }
}

# Helper function for variable extraction
def extract_variable(ens_id, comp, freq, varid):
    # Construct file paths
    fid = casename + '.' + resolution + '.' + expname + '.' + ens_id
    newid = expid + '.' + ens_id
    m1 = ens_id[:2]
    modelid = model[comp]
    din = os.path.join(archive_root, fid, comp, 'hist')
    freqid = freq_name[comp][freq]
    dout = os.path.join(output_root, comp, freq, varid)
    if freq == 'six_hourly':
        dout = dout.replace('six_hourly', 'hourly')
    if freq.startswith('eco_'):
        dout = dout.replace('eco_', '')

    # Loop through years and months
    for yy in range(start_year, end_year + 1):
        for mm in range(1 if yy == start_year else 1, 13):
            extract_data(comp, freq, varid, yy, mm, fid, modelid, din, dout)

def extract_data(comp, freq, varid, yy, mm, fid, modelid, din, dout):
    # Construct input and output file names
    if comp == 'ocn' and (freq == 'daily' or freq == 'eco_daily'):
        finname = os.path.join(din, f'{fid}.{modelid}.{freq_name[comp][freq]}.{yy:04d}-{mm:02d}-01.nc')
    else:
        finname = os.path.join(din, f'{fid}.{modelid}.{freq_name[comp][freq]}.{yy:04d}-{mm:02d}.nc')

    if comp == 'ocn' and freq == 'monthly' and (varid == 'SST' or varid == 'SSS'):
        if yy == start_year and mm == 1:
            subprocess.check_call(['ncks', '-O', '-4', '-D', '0', '-L', '1', '--hst', '-d', 'z_t,0,0', '-v', varid, finname, dout])
        else:
            subprocess.check_call(['ncrcat', '--rec_apn', '--no_tmp_fl', '-4', '-L', '1', '-D', '0', '--hst', '-d', 'z_t,0,0', '-v', varid, finname, dout])
    else:
        if yy == start_year and mm == 1:
            subprocess.check_call(['ncks', '-O', '-4', '-L', '1', '-D', '0', '--hst', '-v', varid, finname, dout])
        else:
            subprocess.check_call(['ncrcat', '--rec_apn', '--no_tmp_fl', '-4', '-L', '1', '--hst', '-D', '0', '-v', varid, finname, dout])

    if (yy == end_year or (yy % 100 == 0)) and comp == 'ocn' and freq == 'monthly' and (varid == 'SST' or varid == 'SSS'):
        subprocess.check_call(['ncrename', '-v', f'{varid},{varid}', dout])

def extvar(ens_id, comp_list):
    for comp in comp_list:
        if extract_type == 'extra':
            var_list = extra_vars[comp][freq]
        else:
            var_list = basic_vars[comp][freq]

        for varid in var_list:
            extract_variable(ens_id, comp, freq, varid)

if __name__ == "__main__":
    tasks = []

    for ens_id in ens_id_list:
        for comp in comp_list:
            comp1_list = [comp, ]
            task = mp.Process(target=extvar, args=(ens_id, comp1_list))
            tasks.append(task)
            task.start()

    for task in tasks:
        task.join()

# End of program