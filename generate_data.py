import pandas as pd
import numpy as np


def generate_np_data():

    num_rows = np.random.randint(5, 500)

    # generate realistic sets of 3 midterm scores for students
    
    # first, generate students' rough average score
    # np generates random number -1 to 1, mean 0, gaussian dist with std dev 1
    predata = np.random.randn(num_rows, 2)
    predata[:, 0] = np.absolute(predata[:, 0]) * 20 + 60 # make one std dev from 60-80
    predata[:, 0] = np.where(predata[:, 0] <= 100, predata[:, 0], 100) # impossible to get >100
    
    # make a student's variation in test scores a function of their "average"
    # (aka better students are more consistent), fun for ML purposes later on
    predata[:, 1] = ((10 - (np.absolute(predata[:, 0]) / 10)) * 2) * (np.absolute(predata[:, 1]) + 2)

    # create raw data array for 3 midterms and pass or fail for final
    data = np.zeros((num_rows, 4))
    data[:, 0] = predata[:, 0]
    data[:, 1] = predata[:, 0]
    data[:, 2] = predata[:, 0]
    split = np.random.randn(num_rows, 1)
    split = np.absolute(split)
    split = np.where(split <= 1, split, 1)

    # first exam is a random portion of their "split" LESS than their rough average
    # third exam is the remaining random portion of their "split" MORE than their rough average
    data[:, 0] -= (predata[:, 1] * split[:, 0])
    data[:, 2] += (predata[:, 1] * (1-split[:, 0]))

    # students end up passing final exam when their average midterm is <70
    data[:, 3] = np.where(data[:, 0] + data[:, 1] + data[:, 2] < 210, 0, 1)
    data = np.round(data, 2)

    # add a 'class year' column 1-4 (freshman-senior)
    class_year = np.random.randn(num_rows, 1)
    class_year = np.absolute(class_year)
    class_year = np.where(class_year < 1, class_year, 1)
    class_year = class_year * 3 + 1
    class_year = np.round(class_year, 0)

    data = np.column_stack((data, class_year))

    return data


def generate_pd_data(np_data):

    # convert numerical data to a pandas df
    pd_data = pd.DataFrame(np_data)

    # add column titles
    pd_data.columns = ["Miderm1", "Midterm2", "Midterm3", "Final Exam Pass?", "Year"]

    # add unique id column
    pd_data["ID"] = pd_data.index
    id_col = pd_data.pop("ID")
    pd_data.insert(0, "ID", id_col)

    # reorder year column
    id_col = pd_data.pop("Year")
    pd_data.insert(1, "Year", id_col)

    return pd_data


def mess_up_data(df):

    messups = [0, 999, pd.NA, None, np.nan]
    

    # number of messed-up data entries
    num_messups = np.random.randint(0, len(df)/20)

    for i in range(num_messups):

        # get a random row
        row_num = np.random.randint(0, len(df))

        # get a random col
        col_num = np.random.randint(0, len(df.columns))
        col_name = df.columns[col_num]

        # item to place
        messup_num = np.random.randint(0, len(messups))
        messup = messups[messup_num]

        # place messup
        df.at[row_num, col_name] = messup

    return df


def generate_csv(df, name):

    df.to_csv('data/' + name, index=False)

def create_csv_data():

    # generate a random number of data files
    num_files = np.random.randint(0, 10)

    for i in range(num_files):

        np_data = generate_np_data()
        pd_data = generate_pd_data(np_data)
        messed_up_pd_data = mess_up_data(pd_data)

        name = "csv_data_" + str(i + 1) + ".csv"

        generate_csv(messed_up_pd_data, name)



create_csv_data()



