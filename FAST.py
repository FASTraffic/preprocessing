import pandas as pd
import numpy as np
# from tqdm import tqdm
import pytz
import datetime
import ast
from datetime import timedelta
from functools import reduce
import matplotlib.pyplot as plt

def preprocess_raw_data(file_path):
    import datetime
    import pytz
    def time_trans(dt_str):
        # Define the date-time string
        # Convert the string to a datetime object in CDT
        dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        #     cdt = pytz.timezone('America/Chicago')
        #     dt = cdt.localize(dt)
        # Convert to UTC and get the Unix timestamp
        unix_timestamp = dt.astimezone(pytz.UTC).timestamp()
        return unix_timestamp
    data = pd.read_csv(file_path)
    return data


def time_trans(dt_str):
    # Define the date-time string
    # Convert the string to a datetime object in CDT
    dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    #     cdt = pytz.timezone('America/Chicago')
    #     dt = cdt.localize(dt)
    # Convert to UTC and get the Unix timestamp
    unix_timestamp = dt.astimezone(pytz.UTC).timestamp()
    return unix_timestamp


def round_to_fix(time):
    seconds = time.second
    microseconds = time.microsecond
    total_seconds = seconds + microseconds / 1_000_000.0

    if total_seconds % 60 < 15:
        new_second = 0
    elif 15 <= total_seconds % 60 < 45:
        new_second = 30
    else:
        new_second = 0
        time = time + timedelta(minutes=1)

    return time.replace(second=new_second, microsecond=0).timestamp()


def extract_lane_speed(lane_dict_str, lane_num):
    # Convert the string representation of dictionary to actual dictionary
    lane_dict = ast.literal_eval(lane_dict_str)

    # Search for the lane and extract the speed
    for value in lane_dict.values():
        if f"Lane{lane_num}" in value[0] and len(value) > 1:
            return value[1]
    return None


def extract_lane_volume(lane_dict_str, lane_num):
    # Convert the string representation of dictionary to actual dictionary
    lane_dict = ast.literal_eval(lane_dict_str)

    # Search for the lane and extract the speed
    for value in lane_dict.values():
        if f"Lane{lane_num}" in value[0] and len(value) > 1:
            return value[2]
    return None


def extract_lane_occupancy(lane_dict_str, lane_num):
    # Convert the string representation of dictionary to actual dictionary
    lane_dict = ast.literal_eval(lane_dict_str)

    # Search for the lane and extract the speed
    for value in lane_dict.values():
        if f"Lane{lane_num}" in value[0] and len(value) > 1:
            return value[3]
    return None

def read_data_fix_time(file_path):
    data = pd.read_csv(file_path)
    data['time_unix'] = data['link_update_time'].apply(lambda x: time_trans(x[:19]))
    data['time_unix_fix'] = data['time_unix'].apply(lambda x: round_to_fix(datetime.datetime.fromtimestamp(x)))
    return data

def raw_lane_level_df_process(data):
    def extract_lane_feature(lane_dict_str, lane_num, feature_index):
        lane_dict = ast.literal_eval(lane_dict_str)
        for value in lane_dict.values():
            if f"Lane{lane_num}" in value[0] and len(value) > feature_index:
                return value[feature_index]
        return None
    def extract_lane_df(data, lane_number):
        lane = data[data['lane_dict_text'].str.contains(f"Lane{lane_number}")].copy()
        lane[f'lane{lane_number}_speed'] = lane['lane_dict_text'].apply(lambda x: extract_lane_feature(x, lane_number, 1))
        lane[f'lane{lane_number}_volume'] = lane['lane_dict_text'].apply(lambda x: extract_lane_feature(x, lane_number, 2))
        lane[f'lane{lane_number}_occ'] = lane['lane_dict_text'].apply(lambda x: extract_lane_feature(x, lane_number, 3))
        return lane[['time_unix_fix', 'milemarker', f'lane{lane_number}_speed', f'lane{lane_number}_volume', f'lane{lane_number}_occ']]
    # Function to merge multiple DataFrames
    def merge_dataframes(dfs, join_keys, how='outer'):
        return reduce(lambda left, right: pd.merge(left, right, on=join_keys, how=how), dfs)
    # List of DataFrames to merge
    lane_dataframes = [extract_lane_df(data, i) for i in range(1, 5)]
    # Keys to merge on
    join_keys = ['time_unix_fix', 'milemarker']
    # Merging all lanes
    all_lanes = merge_dataframes(lane_dataframes, join_keys)
    # to create another with milemaker and time_unix_fix[:-1]
    unique_milemarkers = all_lanes['milemarker'].unique()
    # Extract unique time_unix_fix, excluding the last one
    unique_time_unix_fix = np.sort(all_lanes['time_unix_fix'].unique())[:-1]
    # Create a DataFrame for each unique value
    milemarker_df = pd.DataFrame({'milemarker': unique_milemarkers})
    time_unix_fix_df = pd.DataFrame({'time_unix_fix': unique_time_unix_fix})
    # Perform a cross join to get all combinations
    all_lanes_clean = milemarker_df.merge(time_unix_fix_df, how='cross')
    all_lanes_unique = all_lanes.drop_duplicates(subset=['milemarker', 'time_unix_fix'])
    merged = pd.merge(all_lanes_clean, all_lanes_unique, on=['milemarker', 'time_unix_fix'], how='left')
    for i in range(1, 5):
        speed_col = f'lane{i}_speed'
        volume_col = f'lane{i}_volume'
        occ_col = f'lane{i}_occ'
        merged.loc[merged[speed_col].isna(), [volume_col, occ_col]] = np.nan
    for feature in ['speed', 'volume', 'occ']:
        columns = [f'lane{i}_{feature}' for i in range(1, 5)]
        # Calculate the mean of all lanes for the current feature, ignoring NaNs
        overall_mean = merged[columns].mean(axis=1, skipna=True)
        for column in columns:
            merged[column].fillna(overall_mean, inplace=True)
    return merged

def matrix_to_coordinates(matrix):
    """
    Converts a 2D matrix (numpy.array) into a list of coordinates with values.

    This function iterates through each element of a 2D matrix and
    creates a list of coordinates, where each coordinate is represented
    as a list containing the row index, column index, and the value at
    that position in the matrix.

    :param matrix: A numpy.array where each sublist represents a row in the matrix.
    :type matrix: numpy.array filled with float
    :return: A list of coordinates, where each coordinate is a list of [row_index, column_index, value].
    :rtype: list of list of float

    Example:
        matrix = np.array([
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ])

        coords = matrix_to_coordinates(matrix)
        print(coords)  # Output: [[0, 0, 1], [0, 1, 2], [0, 2, 3], [1, 0, 4], [1, 1, 5], [1, 2, 6], [2, 0, 7], [2, 1, 8], [2, 2, 9]]
    """
    coordinates = []
    for i in range(len(matrix)):
        for j in range(len(matrix[i])):
            coordinates.append([i, j, matrix[i][j]])
    return coordinates





def add_bounded_edges(matrix, boundary_value, row_boundary_thickness, col_boundary_thickness):
    original_rows, original_cols = matrix.shape
    new_rows = original_rows + 2 * row_boundary_thickness
    new_cols = original_cols + 2 * col_boundary_thickness

    # Create a new matrix filled with the boundary value
    new_matrix = np.full((new_rows, new_cols), boundary_value)

    # Insert the original matrix into the center of the new matrix
    new_matrix[row_boundary_thickness:row_boundary_thickness + original_rows,
               col_boundary_thickness:col_boundary_thickness + original_cols] = matrix

    return new_matrix

def asm_data_w_x(processed_data):
    delta = 0.8
    dx = 0.1
    dt = 10
    c_cong = 13
    c_free = -60
    t = abs(delta / c_cong / 2)
    x_mat = 2 * int(delta / dx / 2) + 1
    t_mat = int(t / dt * 3600) * 2 + 1
    matrix = np.zeros([x_mat, t_mat])
    matrix_df = pd.DataFrame(matrix)
    st_df = matrix_df.stack().reset_index()
    st_df.columns = ['x', 't', 'weight']
    st_df['time'] = dt * (st_df['t'] - int(t_mat / 2))
    st_df['space'] = dx * (st_df['x'] - int(x_mat / 2))
    # Define the variables speed and tau
    tau = 90

    # Function to fill weight based on the updated condition using speed and tau
    def fill_cong_weight(row):
        t_new = row['time'] - row['space'] / (c_cong / 3600)
        if abs(t_new) < tau / 2:
            return np.exp(-(abs(t_new) / tau + abs(row['space']) / delta))
        else:
            return 0

    def fill_free_weight(row):
        t_new = row['time'] - row['space'] / (c_free / 3600)
        if abs(t_new) < tau / 2:
            return np.exp(-(abs(t_new) / tau + abs(row['space']) / delta))
        else:
            return 0

    # Applying the function to the DataFrame
    st_df['cong_weight'] = st_df.apply(fill_cong_weight, axis=1)
    st_df['free_weight'] = st_df.apply(fill_free_weight, axis=1)
    cong_weight_matrix = st_df.pivot(index='t', columns='x', values='cong_weight').values
    free_weight_matrix = st_df.pivot(index='t', columns='x', values='free_weight').values
    half_x_mat = int((cong_weight_matrix.shape[1] - 1) / 2)
    half_t_mat = int((cong_weight_matrix.shape[0] - 1) / 2)
    # Assuming `processed_data` has columns for lane 1, 2, 3, and 4 speed, occupancy, and volume
    lanes = [1, 2, 3, 4]
    data_columns = ['speed', 'occ', 'volume']

    # Create the initial DataFrame for each lane and data type
    data = processed_data[
        ['milemarker', 'time_unix_fix'] + [f'lane{lane}_{col}' for lane in lanes for col in data_columns]]

    # Convert time_unix to datetime
    # Determine the range for the milemarker and time
    min_milemarker = data['milemarker'].min()
    max_milemarker = data['milemarker'].max()
    min_time_unix = data['time_unix_fix'].min()
    max_time_unix = data['time_unix_fix'].max()

    # Create a grid for the space (milemarkers) and time (in seconds)
    milemarkers = np.arange(min_milemarker, max_milemarker, 0.1)
    # Create a time range with a 4-second interval in Unix time
    time_range_unix = np.arange(min_time_unix, max_time_unix, 10)
    space_time_matrix_unix = pd.DataFrame(index=time_range_unix, columns=milemarkers)

    # Function to fill the space-time matrix for a given lane and data type
    def fill_space_time_matrix(data, lane, data_type):
        matrix = space_time_matrix_unix.copy()
        for index, row in data.iterrows():
            time_index = row['time_unix_fix']
            milemarker_index = row['milemarker']
            # Find the nearest time and milemarker grid points
            nearest_time = matrix.index.get_indexer([time_index], method='nearest')[0]
            nearest_milemarker = matrix.columns.get_indexer([milemarker_index], method='nearest')[0]
            # Assign the value to the nearest grid point
            matrix.iloc[nearest_time, nearest_milemarker] = row[f'lane{lane}_{data_type}']
        return matrix

    # Create smoothed data matrices for all lanes and data types
    smoothed_data = {}
    vf_data_all = {}
    vc_data_all = {}
    for lane in lanes:
        for data_type in data_columns:
            print(f'Processing lane {lane} {data_type}...')
            if data_type == 'speed':
                space_time_matrix = fill_space_time_matrix(data, lane, data_type)
                pre_smoothed_data = pd.DataFrame(space_time_matrix.values)

                # Perform smoothing
                pre_smoothed_data_w_bound = add_bounded_edges(pre_smoothed_data, np.nan, half_t_mat, half_x_mat)
                smooth_data = np.zeros(pre_smoothed_data.shape)
                vc_data = np.zeros(pre_smoothed_data.shape)
                vf_data = np.zeros(pre_smoothed_data.shape)
                for time_idx in range(pre_smoothed_data.shape[0]):
                    for space_idx in range(pre_smoothed_data.shape[1]):
                        neighbour_matrix = pre_smoothed_data_w_bound[time_idx:time_idx + 2 * half_t_mat + 1,
                                           space_idx:space_idx + 2 * half_x_mat + 1]
                        mask = pd.DataFrame(neighbour_matrix).notna().astype(int).values
                        neighbour_fillna = pd.DataFrame(neighbour_matrix).fillna(0).values
                        N_cong = np.sum(np.multiply(mask, cong_weight_matrix))
                        N_free = np.sum(np.multiply(mask, free_weight_matrix))
                        if N_cong == 0:
                            v_cong = np.nan
                        else:
                            v_cong = np.sum(np.multiply(neighbour_fillna, cong_weight_matrix)) / N_cong
                        if N_free == 0:
                            v_free = np.nan
                        else:
                            v_free = np.sum(np.multiply(neighbour_fillna, free_weight_matrix)) / N_free
                        if N_cong != 0 and N_free != 0:
                            w = 0.5 * (1 + np.tanh((37.29 - min(v_cong, v_free)) / 12.43))
                            v = w * v_cong + (1 - w) * v_free
                        elif N_cong == 0:
                            v = v_free
                        elif N_free == 0:
                            v = v_cong
                        elif N_cong == 0 and N_free == 0:
                            v = np.nan
                        smooth_data[time_idx][space_idx] = v
                        vc_data[time_idx][space_idx] = v_cong
                        vf_data[time_idx][space_idx] = v_free
                smoothed_data[(lane, data_type)] = smooth_data
                vc_data_all[(lane, data_type)] = vc_data
                vf_data_all[(lane, data_type)] = vf_data
            if data_type != 'speed':
                space_time_matrix = fill_space_time_matrix(data, lane, data_type)
                speed_matrix = fill_space_time_matrix(data, lane, 'speed')
                pre_smoothed_data = pd.DataFrame(space_time_matrix.values)
                pre_smoothed_data_speed = pd.DataFrame(speed_matrix.values)
                # Perform smoothing
                pre_smoothed_data_w_bound = add_bounded_edges(pre_smoothed_data, np.nan, half_t_mat, half_x_mat)
                pre_smoothed_data_speed_w_bound = add_bounded_edges(pre_smoothed_data_speed, np.nan, half_t_mat, half_x_mat)
                smooth_data = np.zeros(pre_smoothed_data.shape)
                vc_data = np.zeros(pre_smoothed_data.shape)
                vf_data = np.zeros(pre_smoothed_data.shape)
                for time_idx in range(pre_smoothed_data.shape[0]):
                    for space_idx in range(pre_smoothed_data.shape[1]):
                        neighbour_matrix = pre_smoothed_data_w_bound[time_idx:time_idx + 2 * half_t_mat + 1,
                                           space_idx:space_idx + 2 * half_x_mat + 1]
                        neigbour_matrix_speed = pre_smoothed_data_speed_w_bound[time_idx:time_idx + 2 * half_t_mat + 1,
                                                  space_idx:space_idx + 2 * half_x_mat + 1]
                        mask = pd.DataFrame(neighbour_matrix).notna().astype(int).values
                        mask_speed = pd.DataFrame(neigbour_matrix_speed).notna().astype(int).values
                        neighbour_fillna = pd.DataFrame(neighbour_matrix).fillna(0).values
                        neighbour_fillna_speed = pd.DataFrame(neigbour_matrix_speed).fillna(0).values
                        N_cong = np.sum(np.multiply(mask, cong_weight_matrix))
                        N_free = np.sum(np.multiply(mask, free_weight_matrix))
                        N_cong_speed = np.sum(np.multiply(mask_speed, cong_weight_matrix))
                        N_free_speed = np.sum(np.multiply(mask_speed, free_weight_matrix))
                        if N_cong == 0:
                            v_cong = np.nan
                        else:
                            v_cong = np.sum(np.multiply(neighbour_fillna, cong_weight_matrix)) / N_cong
                            v_cong_speed = np.sum(np.multiply(neighbour_fillna_speed, cong_weight_matrix)) / N_cong_speed
                        if N_free == 0:
                            v_free = np.nan
                        else:
                            v_free = np.sum(np.multiply(neighbour_fillna, free_weight_matrix)) / N_free
                            v_free_speed = np.sum(np.multiply(neighbour_fillna_speed, free_weight_matrix)) / N_free_speed
                        if N_cong != 0 and N_free != 0:
                            w = 0.5 * (1 + np.tanh((37.29 - min(v_cong_speed, v_free_speed)) / 12.43))
                            v = w * v_cong + (1 - w) * v_free
                        elif N_cong == 0:
                            v = v_free
                        elif N_free == 0:
                            v = v_cong
                        elif N_cong == 0 and N_free == 0:
                            v = np.nan
                        smooth_data[time_idx][space_idx] = v
                        vc_data[time_idx][space_idx] = v_cong
                        vf_data[time_idx][space_idx] = v_free
                smoothed_data[(lane, data_type)] = smooth_data
                vc_data_all[(lane, data_type)] = vc_data
                vf_data_all[(lane, data_type)] = vf_data

    # Convert the smoothed data to coordinates
    result_dfs = []
    for lane in lanes:
        for data_type in data_columns:
            smooth_data = smoothed_data[(lane, data_type)]
            vc_data = vc_data_all[(lane, data_type)]
            vf_data = vf_data_all[(lane, data_type)]
            smooth_data_df = pd.DataFrame(matrix_to_coordinates(smooth_data))
            vc_data_df = pd.DataFrame(matrix_to_coordinates(vc_data))
            vf_data_df = pd.DataFrame(matrix_to_coordinates(vf_data))
            smooth_data_df.columns = ['time_index', 'space_index', f'lane{lane}_{data_type}']
            vc_data_df.columns = ['time_index', 'space_index', f'vc_lane{lane}_{data_type}']
            vf_data_df.columns = ['time_index', 'space_index', f'vf_lane{lane}_{data_type}']
            smooth_data_df['unix_time'] = min_time_unix + smooth_data_df['time_index'] * dt
            smooth_data_df['milemarker'] = min_milemarker + smooth_data_df['space_index'] * dx
            # add two more columns from vc and vf that the lane data type to smooth_data_df
            smooth_data_df = smooth_data_df.merge(vc_data_df, on=['time_index', 'space_index'], how='left')
            smooth_data_df = smooth_data_df.merge(vf_data_df, on=['time_index', 'space_index'], how='left')
            result_dfs.append(smooth_data_df[['unix_time', 'milemarker', f'lane{lane}_{data_type}',
                                              f'vc_lane{lane}_{data_type}', f'vf_lane{lane}_{data_type}']])

    # Concatenate all results into a single DataFrame
    final_df = pd.concat(result_dfs, axis=1)
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    return final_df

def asm_data_raw_res(processed_data):
    delta = 1
    dx = 0.3
    dt = 30
    c_cong = 13
    c_free = -60
    t = abs(delta / c_cong / 2)
    x_mat = 2 * int(delta / dx / 2) + 1
    t_mat = int(t / dt * 3600) * 2 + 1
    matrix = np.zeros([x_mat, t_mat])
    matrix_df = pd.DataFrame(matrix)
    st_df = matrix_df.stack().reset_index()
    st_df.columns = ['x', 't', 'weight']
    st_df['time'] = dt * (st_df['t'] - int(t_mat / 2))
    st_df['space'] = dx * (st_df['x'] - int(x_mat / 2))
    # Define the variables speed and tau
    tau = 90

    # Function to fill weight based on the updated condition using speed and tau
    def fill_cong_weight(row):
        t_new = row['time'] - row['space'] / (c_cong / 3600)
        if abs(t_new) < tau / 2:
            return np.exp(-(abs(t_new) / tau + abs(row['space']) / delta))
        else:
            return 0

    def fill_free_weight(row):
        t_new = row['time'] - row['space'] / (c_free / 3600)
        if abs(t_new) < tau / 2:
            return np.exp(-(abs(t_new) / tau + abs(row['space']) / delta))
        else:
            return 0

    # Applying the function to the DataFrame
    st_df['cong_weight'] = st_df.apply(fill_cong_weight, axis=1)
    st_df['free_weight'] = st_df.apply(fill_free_weight, axis=1)
    cong_weight_matrix = st_df.pivot(index='t', columns='x', values='cong_weight').values
    free_weight_matrix = st_df.pivot(index='t', columns='x', values='free_weight').values
    half_x_mat = int((cong_weight_matrix.shape[1] - 1) / 2)
    half_t_mat = int((cong_weight_matrix.shape[0] - 1) / 2)
    # Assuming `processed_data` has columns for lane 1, 2, 3, and 4 speed, occupancy, and volume
    lanes = [1, 2, 3, 4]
    data_columns = ['speed', 'occ', 'volume']

    # Create the initial DataFrame for each lane and data type
    data = processed_data[
        ['milemarker', 'time_unix_fix'] + [f'lane{lane}_{col}' for lane in lanes for col in data_columns]]

    # Convert time_unix to datetime
    # Determine the range for the milemarker and time
    min_milemarker = data['milemarker'].min()
    max_milemarker = data['milemarker'].max() + 0.1
    min_time_unix = data['time_unix_fix'].min()
    max_time_unix = data['time_unix_fix'].max() + 30

    # Create a grid for the space (milemarkers) and time (in seconds)
    milemarkers = np.arange(min_milemarker, max_milemarker+0.1, 0.3)
    # Create a time range with a 4-second interval in Unix time
    time_range_unix = np.arange(min_time_unix, max_time_unix, 30)
    space_time_matrix_unix = pd.DataFrame(index=time_range_unix, columns=milemarkers)

    # Function to fill the space-time matrix for a given lane and data type
    def fill_space_time_matrix(data, lane, data_type):
        matrix = space_time_matrix_unix.copy()
        for index, row in data.iterrows():
            time_index = row['time_unix_fix']
            milemarker_index = row['milemarker']
            # Find the nearest time and milemarker grid points
            nearest_time = matrix.index.get_indexer([time_index], method='nearest')[0]
            nearest_milemarker = matrix.columns.get_indexer([milemarker_index], method='nearest')[0]
            # Assign the value to the nearest grid point
            matrix.iloc[nearest_time, nearest_milemarker] = row[f'lane{lane}_{data_type}']
        return matrix
    # Create smoothed data matrices for all lanes and data types
    smoothed_data = {}
    vf_data_all = {}
    vc_data_all = {}
    for lane in lanes:
        for data_type in data_columns:
            print(f'Processing lane {lane} {data_type}...')
            space_time_matrix = fill_space_time_matrix(data, lane, data_type)
            pre_smoothed_data = pd.DataFrame(space_time_matrix.values)

            # Perform smoothing
            pre_smoothed_data_w_bound = add_bounded_edges(pre_smoothed_data, np.nan, half_t_mat, half_x_mat)
            smooth_data = np.zeros(pre_smoothed_data.shape)
            vc_data = np.zeros(pre_smoothed_data.shape)
            vf_data = np.zeros(pre_smoothed_data.shape)
            for time_idx in range(pre_smoothed_data.shape[0]):
                for space_idx in range(pre_smoothed_data.shape[1]):
                    neighbour_matrix = pre_smoothed_data_w_bound[time_idx:time_idx + 2 * half_t_mat + 1,
                                       space_idx:space_idx + 2 * half_x_mat + 1]
                    mask = pd.DataFrame(neighbour_matrix).notna().astype(int).values
                    neighbour_fillna = pd.DataFrame(neighbour_matrix).fillna(0).values
                    N_cong = np.sum(np.multiply(mask, cong_weight_matrix))
                    N_free = np.sum(np.multiply(mask, free_weight_matrix))
                    if N_cong == 0:
                        v_cong = np.nan
                    else:
                        v_cong = np.sum(np.multiply(neighbour_fillna, cong_weight_matrix)) / N_cong
                    if N_free == 0:
                        v_free = np.nan
                    else:
                        v_free = np.sum(np.multiply(neighbour_fillna, free_weight_matrix)) / N_free
                    if N_cong != 0 and N_free != 0:
                        w = 0.5 * (1 + np.tanh((37.29 - min(v_cong, v_free)) / 12.43))
                        v = w * v_cong + (1 - w) * v_free
                    elif N_cong == 0:
                        v = v_free
                    elif N_free == 0:
                        v = v_cong
                    elif N_cong == 0 and N_free == 0:
                        v = np.nan
                    smooth_data[time_idx][space_idx] = v
                    vc_data[time_idx][space_idx] = v_cong
                    vf_data[time_idx][space_idx] = v_free
            smoothed_data[(lane, data_type)] = smooth_data
            vc_data_all[(lane, data_type)] = vc_data
            vf_data_all[(lane, data_type)] = vf_data

    # Convert the smoothed data to coordinates
    result_dfs = []
    for lane in lanes:
        for data_type in data_columns:
            smooth_data = smoothed_data[(lane, data_type)]
            vc_data = vc_data_all[(lane, data_type)]
            vf_data = vf_data_all[(lane, data_type)]
            smooth_data_df = pd.DataFrame(matrix_to_coordinates(smooth_data))
            vc_data_df = pd.DataFrame(matrix_to_coordinates(vc_data))
            vf_data_df = pd.DataFrame(matrix_to_coordinates(vf_data))
            smooth_data_df.columns = ['time_index', 'space_index', f'lane{lane}_{data_type}']
            vc_data_df.columns = ['time_index', 'space_index', f'vc_lane{lane}_{data_type}']
            vf_data_df.columns = ['time_index', 'space_index', f'vf_lane{lane}_{data_type}']
            smooth_data_df['unix_time'] = min_time_unix + smooth_data_df['time_index'] * dt
            smooth_data_df['milemarker'] = min_milemarker + smooth_data_df['space_index'] * dx
            # add two more columns from vc and vf that the lane data type to smooth_data_df
            smooth_data_df = smooth_data_df.merge(vc_data_df, on=['time_index', 'space_index'], how='left')
            smooth_data_df = smooth_data_df.merge(vf_data_df, on=['time_index', 'space_index'], how='left')
            result_dfs.append(smooth_data_df[['unix_time', 'milemarker', f'lane{lane}_{data_type}',
                                              f'vc_lane{lane}_{data_type}', f'vf_lane{lane}_{data_type}']])

    # Concatenate all results into a single DataFrame
    final_df = pd.concat(result_dfs, axis=1)
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    return final_df
def days_since_2020(input_date):
    """
    Calculate the number of days between the given date and January 1, 2020.

    Parameters:
    input_date (str): The date in 'YYYY-MM-DD' format.

    Returns:
    int: The number of days between input_date and January 1, 2020.
    """
    date_format = "%Y-%m-%d"
    start_date = datetime.datetime.strptime("2020-01-01", date_format)
    given_date = datetime.datetime.strptime(input_date, date_format)
    delta = given_date - start_date
    return delta.days


def visualize_data(smoothed_data, lane_number, measurement, figsize=(50, 10), vmin=0, vmax=80, cmap='hot', s=2,
                   save_fig=False, figure_root='figure', dpi=300):
    plt.rcParams.update({
        "text.usetex": True,
        "font.family": "serif",
        "font.size": 40
    })

    plt.figure(figsize=figsize)
    plt.scatter(smoothed_data['unix_time'],
                smoothed_data['milemarker'],
                c=smoothed_data[f'lane{lane_number}_{measurement}'],
                vmin=vmin, vmax=vmax,
                cmap=cmap, s=s)
    plt.colorbar(label=f'Lane {lane_number} {measurement.capitalize()}')
    plt.xlabel('Unix Time')
    plt.ylabel('Mile Marker')
    plt.title(f'Visualization of Lane {lane_number} {measurement.capitalize()}')
    plt.tight_layout()

    if save_fig:
        plt.savefig(f'{figure_root}_lane{lane_number}_{measurement}.png', dpi=dpi, bbox_inches='tight')

    plt.show()


import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pytz


def visualize_data_matrix(smoothed_data, lane_number, measurement, figsize=(20, 10), vmin=0, vmax=80, cmap='hot',
                          save_fig=False, figure_root='figure', dpi=300):
    plt.rcParams.update({
        "text.usetex": True,
        "font.family": "serif",
        "font.size": 40
    })
    data = smoothed_data.copy()
    # Set the value range based on the measurement type
    if measurement == 'occ':
        vmin, vmax = 0, 100
        cmap = 'hot_r'
        colorbar_label = 'Occupancy (%)'
    elif measurement == 'volume':
        vmin, vmax = 0, 3000
        data[f'lane{lane_number}_volume'] *= 120
        cmap = 'hot_r'
        colorbar_label = 'Volume (veh/h)'
    else:  # Default case is for speed
        vmin, vmax = 0, 80
        colorbar_label = 'Speed (mph)'

    # Get min and max unix_time
    min_unix_time = data['unix_time'].min()
    max_unix_time = data['unix_time'].max()

    # Convert min and max unix_time to Chicago time
    min_time = pd.to_datetime(min_unix_time, unit='s').tz_localize('UTC').tz_convert('America/Chicago')
    max_time = pd.to_datetime(max_unix_time + 30, unit='s').tz_localize('UTC').tz_convert('America/Chicago')

    # Create a date range for the x-ticks
    time_range = pd.date_range(start=min_time, end=max_time, freq='60T')

    # Pivot the data to create a matrix
    matrix_data = data.pivot(index='milemarker', columns='unix_time', values=f'lane{lane_number}_{measurement}')

    plt.figure(figsize=figsize)
    im = plt.imshow(matrix_data, aspect='auto', cmap=cmap, vmin=vmin, vmax=vmax, origin='lower')
    # plt.colorbar(im, label=colorbar_label)

    # Set x-ticks and labels
    xticks_positions = np.searchsorted(matrix_data.columns, time_range.astype(int) // 10 ** 9)
    xticks_labels = time_range.strftime('%H:%M')
    plt.xticks(ticks=xticks_positions, labels=xticks_labels, rotation=0)
    # add a virtical line to show the time = 8640
    # plt.axvline(x=8640, color='g', linestyle='--')
    # Set y-ticks and labels every 1 mile
    yticks_positions = np.arange(matrix_data.index.min(), matrix_data.index.max() + 0.1, 3)
    plt.yticks(ticks=3.3333333 * (yticks_positions - matrix_data.index.min()), labels=yticks_positions)
    date_str = min_time.date().strftime('%Y-%m-%d')
    plt.xlabel(f'Time ({date_str})')
    plt.ylabel('Mile Marker')
    # plt.title(f'Visualization of Lane {lane_number} {measurement.capitalize()}')
    # y label inverse
    plt.gca().invert_yaxis()
    plt.tight_layout()

    if save_fig:
        date_str = min_time.date().strftime('%Y-%m-%d')
        plt.savefig(f'{figure_root}/{date_str}_lane{lane_number}_{measurement}.pdf', dpi=dpi, bbox_inches='tight')

    plt.show()
    print('========', 'Visualization of Lane', lane_number, measurement, 'is done', '========')
