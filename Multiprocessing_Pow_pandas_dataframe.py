import pandas as pd
import numpy as np
import multiprocessing as mp

processors = mp.cpu_count()
mp_dict = mp.Manager().dict()
mp_loop = []

size = 10000000
input_step = int(size / processors)

df = pd.DataFrame(data={
    'RandomNumber': np.random.random(size=size),
    'RandomInt': np.random.randint(0, high=100, size=size)
})


def func(df_, indexlist_, processor_, multi_dict_):
    '''
    This function will raise the RandomNumber in the dataframe by the power of the RandomInt in the dataframe.
    '''
    series_ = pd.Series(
        data=df_.loc[indexlist_, 'RandomNumber'].values ** df_.loc[indexlist_, 'RandomInt'].values,
        index=indexlist_
    )
    multi_dict_[processor_] = series_.to_dict()


for processor_ in range(processors):
    if processor_ == 0:
        index_ = list(range(0, input_step, 1))
    elif processor_ == processors - 1:
        index_ = list(range(processor_ * input_step, size, 1))
    else:
        index_ = list(range(input_step * processor_, input_step * (processor_ + 1), 1))
mp_ = mp.Process(
    target=func,
    args=(df, index_, processor_, mp_dict)
)
mp_loop.append(mp_)
for l_ in mp_loop:
    l_.start()

for l_ in mp_loop:
    l_.join()

df_results = pd.DataFrame([])

for key_ in mp_dict.keys():
    df_results = df_results.append(
        pd.DataFrame.from_dict(
            mp_dict[key_], orient='index', columns=['RandomPower']
        )
    )

df = df.merge(
    df_results,
    how='left',
    left_index=True,
    right_index=True
)
df.head(10)