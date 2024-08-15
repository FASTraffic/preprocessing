import FAST
import warnings

warnings.filterwarnings('ignore')
import os
from multiprocessing import Pool

# Paths
raw_data_path = 'ftaed_data/raw_data/'
process_date_path = 'ftaed_data/processed_data/'
smoothed_data_path = 'ftaed_data/smoothed_data/'
figure_root = 'FAST_figures'

# Get file names and sort
file_names = os.listdir(raw_data_path)
file_names.sort()


# Define the processing function
def process_file(file_name):
    if file_name == '.DS_Store':
        return

    print('========', 'Processing file:', file_name, '========')
    process_date = file_name.split('.')[0]
    day = FAST.days_since_2020(process_date)
    data = FAST.read_data_fix_time(raw_data_path + process_date + '.csv')
    processed_data = FAST.raw_lane_level_df_process(data)
    processed_data.to_csv(process_date_path + process_date + '.csv', index=False)

    # Uncomment these lines if you want to include the smoothed data processing and visualization
    # smoothed_data = FAST.asm_data_raw_res(processed_data)
    # smoothed_data['day'] = day
    # smoothed_data.to_csv(smoothed_data_path + process_date + '.csv', index=False)
    # FAST.visualize_data_matrix(smoothed_data, lane_number=1, measurement='speed', save_fig=True, figure_root=figure_root)
    # FAST.visualize_data_matrix(smoothed_data, lane_number=1, measurement='volume', save_fig=True, figure_root=figure_root)
    # FAST.visualize_data_matrix(smoothed_data, lane_number=1, measurement='occ', save_fig=True, figure_root=figure_root)


# Multiprocessing setup
if __name__ == "__main__":
    with Pool() as pool:
        pool.map(process_file, file_names)