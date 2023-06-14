from requirements import *


def combine_keywords(movie_id):
    try:
        return ','.join(get_keywords_movie(movie_id))
    except:
        return ""


def nonnull_columns(df, column):
    return ~df[column].isnull() & ~(df[column] == '\\N')


def flatten(some_list):
    return [item for sublist in some_list for item in sublist]


def listdir_nohidden(path):
    return glob.glob(os.path.join(path, '*'))


def get_keywords_movie(movie_id):
    keywords = Cinemagoer().get_movie(movie_id.split("tt")[-1], info='keywords').get('keywords')
    if keywords is None:
        return []
    else:
        return keywords


def reset_and_copy(df):
    return df.copy().reset_index(drop=True)


def get_dfs_divided(df, chunks=5):
    return [df.loc[indexes] for indexes in divide_chunks(df.index, round(df.index.size / chunks))]


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_chunks(df):
    return int(df.shape[0] / 1000) + 1


def link_url_dups(df):
    return df['link url'][df['link url'].duplicated()]


def filter_df(df):
    return df.loc[~df['link url'].isin(link_url_dups(df)) & df['duration'] != 0]


def get_soup_url_w_proxy(url, proxy):
    page_content = get_page_url_w_proxy(url, proxy)
    return BeautifulSoup(page_content.content.decode('utf-8', 'ignore'))

def get_soup_url(url, proxies=None):
    try:
        return BeautifulSoup(get_page_url(url, proxies).content, "html.parser")
    except:
        return BeautifulSoup(get_page_url(url, proxies).content.decode('utf-8', 'ignore'))

def get_page_url(url, proxies=None):
    if proxies is not None:
        return requests.get(url, proxies={'http': sample(proxies, 1)[0], 'https': sample(proxies, 1)[0]})
    else:
        return requests.get(url)

def get_page_url_w_proxy(url, proxy):
    return requests.get(url, timeout=10, proxies={'http': f"http://{proxy.get('host')}:{proxy.get('port')}"})

def extract_file(file_to_extract, output_file):
    with gzip.open(file_to_extract, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def download_file(file_name, url):
    try:
        r = requests.get(url, stream=True)
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("%s downloaded!\n" % file_name)
    except:
        print("An Error Downloading %s" % file_name)


def minutes_from_string(date_string):
    try:
        split_string = [int(split_str) for split_str in date_string.split(":")]
        if len(split_string) == 2:
            return round(split_string[0] + (split_string[1] / 60))
        elif len(split_string) == 3:
            return round((split_string[0] * 60) + split_string[1] + (split_string[2] / 60))
        else:
            return 0
    except:
        return 0


def column_not_available(df, column):
    return column not in df.columns


def add_column_to_data(df, input_column, output_column):
    if column_not_available(df, output_column):
        df[output_column] = df[input_column]


def soup_from_path(path):
    with open(path) as fp:
        soup = BeautifulSoup(fp, 'html.parser')
    return soup


def get_list_proxies(number, https=True):
    proxies = []
    count = 0
    while count < number:
        if count % 10 == 0:
            print('finding the #{} proxy'.format(count))
        try:
            proxies.append(FreeProxy(https=https).get())
            count += 1
        except Exception as e:
            print('exception error is', e)
            pass
    return proxies


def searchable_movie_name(movie_name):
    movie_name = re.sub(":", "", movie_name)
    movie_name = re.sub("'", "", movie_name)
    movie_name = re.sub("\.\.\.", "", movie_name)
    movie_name = re.sub("\.\.", "", movie_name)
    movie_name = re.sub("_", " ", movie_name)
    movie_name = re.sub("-", " ", movie_name)
    movie_name = re.sub("  ", " ", movie_name)
    movie_name = re.sub("\.", " ", movie_name)
    movie_name = re.sub('"', "", movie_name)
    movie_name = re.sub("\*", "", movie_name)
    return movie_name.lower().strip()


def year():
    return r'(18[8-9][0-9]|19[0-9]{2}|20[0-1][0-9]|202[0-3])'


def file_updated_time(file_path):
    return os.path.getmtime(file_path)


def load_yaml_file(file):
    with open(file) as f:
        data = yaml.load(f, Loader=SafeLoader)
    return data


def set_key(dict_object, key, key_val, excluded_keys=[]):
    if not_found(key, excluded_keys):
        if type(key_val) in [str, int, float]:
            dict_object[key] = key_val
        elif type(key_val) == list:
            [set_key(dict_object, "{}_{}".format(key, index + 1), key_val[index], excluded_keys) for index in
             range(len(key_val))]
        elif type(key_val) == dict:
            [set_key(dict_object, "{}_{}".format(key, sub_key), key_val.get(sub_key), excluded_keys) for sub_key in
             key_val.keys()]


def not_found(key, excluded_keys):
    return not any([excluded in key for excluded in excluded_keys])


def file_has_data(file_path):
    return os.stat(file_path).st_size > 1


def filtered_file_paths(directory):
    return list(filter(file_has_data, listdir_nohidden(directory)))


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def merged_dicts(the_list, item_function=None):
    if item_function is None:
        return dict(ChainMap(*[item for item in the_list]))
    return dict(ChainMap(*[item_function(item) for item in the_list]))


def get_driver(use_proxy, window_size, chromedriver_path):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=%s" % window_size)
    if use_proxy:
        proxy = FreeProxy(elite=True).get()
        options.add_argument('--proxy-server=%s' % proxy)
    return webdriver.Chrome(executable_path=chromedriver_path, chrome_options=options)

def get_undetected_driver(use_proxy, test=False):
    if not test:
        options.add_argument("--headless")
    options = uc.ChromeOptions()
    driver = uc.Chrome(options=options) 

def has_some_data(data):
    try:
        return len(data) > 0
    except:
        return False


def empty_column_value(row, col):
    return ( pd.isnull(row[col]) ) or ( row[col] == "\\N" ) or ( row[col] in ['None', None] )

def not_empty_column(row, col):
    return not empty_column_value(row, col)

def get_year(title):
    matches = re.findall(year(), str(title))
    if len(matches) == 1:
        return matches[0]
    else:
        return None

def try_except_method(function, *function_args, return_value=None):
    try:
        return function(function_args)
    except:
        return return_value

def get_first_key_from_list(a_dict):
    return list(a_dict.keys())[0]


def convert_xy_columns(df):
    xy_columns = [column for column in df.columns if column.endswith('_x') or column.endswith('_y')]
    for column in xy_columns:
        for split_var in ['_x', '_y', '_z']:
            if split_var in column:
                column_split = split_var
        new_column = column.split(column_split)[0]
        df[new_column] = df.loc[nonnull_columns(df, column), [column]]
    df.drop(xy_columns, inplace=True, axis=1)


def not_null_value(value):
    return not pd.isnull(value) and value not in ['None', 'nan', None, '\\N']

def column_within_range(df, amount, column_a, column_b):
    column_a_value = df[column_a]
    column_b_value = df[column_b]
    within_greater_amount = (  column_a_value >=  column_b_value - amount)
    within_lesser_amount = ( column_a_value <= column_b_value + amount)
    return within_greater_amount & within_lesser_amount
    
def get_selenium_soup(driver):
    return BeautifulSoup(driver.page_source, "html.parser")

def get_selenium_driver(proxies=None, headless=True):
    try:
        proxy = sample(proxies, 1)[0]
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        if proxies is not None:
            options.add_argument(f"--proxy-server={proxy.get('host')}:{proxy.get('port')}")
        return uc.Chrome(options=options)
    except:
        return get_selenium_driver(proxies, headless)

def try_df(file):
    try:
        return pd.read_csv(file)
    except:
        return None

