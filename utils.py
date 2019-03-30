import datetime
from pyspark.sql import Row


def row_to_dict(row):
    """ Convert a Pyspark row to a native python dictionary """

    response = level = row.asDict()
    working = True
    deeper = False
    depth = []

    while working:
        for key, value in level.items():
            if type(value) is list or type(value) is Row:
                if type(value) == list:
                    if type(value[0]) == Row:
                        value = [val.asDict() for val in value if type(val)==Row]
                    else:
                        value = value
                    nested_set(response, depth + [key], value)

                else:
                    deeper = True
                    descent_key = key
                    nested_set(response, depth + [key], value.asDict())
            else:
                nested_set(response, depth + [key], value)
        if deeper:
            depth.append(descent_key)
            level = level[descent_key]
            deeper = False
        else:
            working = False
    return response


def nested_set(dic, keys, val, dic_response=False):
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    if dic_response:
        return dic
    else:
        dic[keys[-1]] = val


def dict_to_row(dictionary):
    """ Convert a native python dictionary into a Pyspark Row"""

    def d_2_r(d):
        return Row(**{key: value for key, value in d.items()})

    response = level = dictionary
    working = True
    deeper = False
    depth = []

    while working:
        for key, value in level.items():
            if type(value) == dict:
                deeper = True
                descent_key = key

        if depth == []:
            response = d_2_r(response)
            working = False
            break

        if deeper:
            depth.append(descent_key)
            level = level[descent_key]
            deeper = False
        else:
            nested_set(response, depth, d_2_r(level))
            depth = depth[:-1]
            level = nested_set(response, depth, None, dic_response=True)

    return response


def df_to_rdd(df):
    return df.rdd.map(row_to_dict)


def rdd_to_df(rdd):
    return rdd.map(dict_to_row).toDF()


def cla_timestamp_to_datetime(cla_ts):
    try:
        return datetime.datetime.strptime(cla_ts, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.datetime(year=1, month=1, day=1)