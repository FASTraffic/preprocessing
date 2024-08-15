import FAST
import os
import pandas as pd
from multiprocessing import Pool

# Define paths
file_path = 'ftaed_data/'
if not os.path.exists(file_path):
    os.mkdir(file_path)
raw_data_path = os.path.join(file_path, 'raw_data/')
process_date_path = os.path.join(file_path, 'processed_data/')
smoothed_data_path = os.path.join(file_path, 'smoothed_data/')
figure_root = os.path.join(file_path, 'figures')

for path in [raw_data_path, process_date_path, smoothed_data_path, figure_root]:
    if not os.path.exists(path):
        os.mkdir(path)


# Function to process a single file
def process_file(file_name):
    if file_name == '.DS_Store':
        return
    print('======== Processing file:', file_name, '========')
    process_date = file_name.split('.')[0]
    day = FAST.days_since_2020(process_date)
    processed_data = pd.read_csv(os.path.join(process_date_path, process_date + '.csv'))

    smoothed_data = FAST.asm_data_raw(processed_data)
    smoothed_data['day'] = day
    smoothed_data.to_csv(os.path.join(smoothed_data_path, process_date + '.csv'), index=False)

    FAST.visualize_data_matrix(smoothed_data, lane_number=1, measurement='speed', save_fig=True,
                               figure_root=figure_root)


# Get list of files and sort
file_names = os.listdir(raw_data_path)
file_names.sort()

# Use multiprocessing to process files concurrently
if __name__ == '__main__':
    with Pool() as pool:
        pool.map(process_file, file_names)