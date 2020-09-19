import os
import pyspark
import sys
import time
from csv import reader as Reader

####################################################################################################
# loadDataset(directory, files, sqlContext)
# 
# Loads and parses the dataset into a PySpark dataframe
# 
# parameters:
#     locationsDirectory: folder name of directory holding locations dataset
#     recordingsDirectory: folder name of directory holding recordings dataset
#     sqlContext: PySpark SQL context session
# return:
#     two PySpark dataframes of the locations dataset and the recordings dataset
# 
def loadDataset(locationsDirectory, recordingsDirectory, sqlContext):
    files = ['WeatherStationLocations.csv', '2006.txt', '2007.txt', '2008.txt', '2009.txt']
    if locationsDirectory[-1] != '/':
        locationsDirectory += '/'
    if recordingsDirectory[-1] != '/':
        recordingsDirectory += '/'
    
    locations = []
    USAFs = {}
    with open(locationsDirectory + files[0], 'r') as f:
        reader = Reader(f)
        
        parse = next(reader)
        parse = [parse[0], parse[4]]

        locations = [parse]
        for parse in reader:
            country = parse[3]
            state = parse[4]
            if country == 'US' and state:
                USAF = parse[0]
                
                locations.append([USAF, state])
                USAFs[USAF] = None
                
    locations = sqlContext.createDataFrame(locations[1:], schema = locations[0])
    
    recordings = []
    for file in files[1:]:
        with open(recordingsDirectory + file, 'r') as f:
            if not recordings:
                parse = f.readline().split()
                parse = [parse[0][:-3], 'MONTH', parse[13]]

                recordings = [parse]
                
            for line in f:
                parse = line.split()

                if len(parse) > 16:
                    USAF = parse[0]
                    
                    if USAF in USAFs:
                        precipitation = convertPrecipitation(parse[19])

                        if precipitation is not None:
                            parse = [USAF, int(parse[2][4:6]), precipitation]
                            recordings.append(parse)
                        
    recordings = sqlContext.createDataFrame(recordings[1:], schema = recordings[0])   
        
    return locations, recordings

####################################################################################################
# convertPrecipitation(precipitation)
# 
# Translates the precipitation object to a meaningful data value
# 
# parameters:
#     precipitation: string variable of numerical characters with valid measurements terminating with a letter between A-I
# return:
#     None if the precipitation is invalid
#     Float number for amount of precipitation measured
# 
CONVERSIONS = {'A':4, 'B':2, 'C':4 / 3, 'D':1, 'E':2, 'F':1, 'G':1, 'H':0, 'I':0}
def convertPrecipitation(precipitation):
    factor = precipitation[-1]
    
    if factor not in CONVERSIONS:
        return None
    else:
        measurement = float(precipitation[:-1])
        
        return measurement * CONVERSIONS[factor]

####################################################################################################
# search(table)
# 
# Queries the table to extract the average precipitation for each month along with the maximum and minimum precipitation month and difference for each state
# 
# parameter:
#     table: PySpark dataframe of the natural joined locations and recordings dataset
# 
# return:
#     sorted list of states and the average precipitation for each month along with the maximum and minimum precipitation month and difference ordered by their difference
# 
def search(table):
    MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    states = {}
    for i, month in enumerate(MONTHS, 1):
        results = table.where(table.MONTH == i).groupBy('STATE').avg('PRCP').collect()
        
        for result in results:
            state = result[0]
            average = result[1]

            if state in states:
                if average >= states[state]['max'][1]:
                    states[state]['max'] = [month, average]
                    states[state]['diff'] = abs(average - states[state]['min'][1])
                elif average <= states[state]['min'][1]:
                    states[state]['min'] = [month, average]
                    states[state]['diff'] = abs(average - states[state]['max'][1])
            else:
                states[state] = {'max':[month, average], 'min':[month, average], 'diff':0}

    return sorted(states.items(), key = lambda x: x[1]['diff'])

print('=' * 25, 'Weather Locations and Recordings Project', '=' * 25, '\n')
print('[Building Spark Context Session]\n')

sparkContext = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sparkContext)

locationsDirectory = sys.argv[1]
recordingsDirectory = sys.argv[2]

print('[Extracting Datasets]\n')

start = time.time()

locations, recordings = loadDataset(locationsDirectory, recordingsDirectory, sqlContext)

print('[Querying Data]')

joined = locations.join(recordings, locations.USAF == recordings.STN)

results = search(joined)

end = time.time()

outputDirectory = sys.argv[3]
if not os.path.exists(outputDirectory):
    os.makedirs(outputDirectory)
if outputDirectory[-1] != '/':
    outputDirectory += '/'
with open(outputDirectory + 'results.txt', 'w+') as f:
    print('Weather Locations and Recordings Results\n')

    print('Elapsed time:', end - start, 'seconds\n')

    print('{:10}{:<40}{:<40}{:<28}'.format('State', 'Maximum', 'Minimum', 'Difference'))
    print('-' * 120)
    for result in results:
        f.write(result[0] + '\n')
        f.write(str(result[1]['max'][1]) + ', ' + result[1]['max'][0] + '\n')
        f.write(str(result[1]['min'][1]) + ', ' + result[1]['min'][0] + '\n')
        f.write(str(result[1]['diff']) + '\n')
        print('{:10}{:<12}{:<28}{:<12}{:<28}{:<28}'.format(result[0], result[1]['max'][0], result[1]['max'][1], result[1]['min'][0], result[1]['min'][1], result[1]['diff']))