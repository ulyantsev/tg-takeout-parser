# A simple script to parse telegram takeout json file to pandas dataframe and generate some statistics and plots.
# Free to use, no warranty, no license. Modify as you wish.
# author: @ulyantsev
import datetime

import ijson
import sys
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


CHAT_TYPES = ['private_group', 'personal_chat']

MESSAGE_RECORDS = {'id': 'int',
                   'date': 'datetime64[ns]',
                   'type': 'category',
                   'from_id': 'category',
                   'text': 'string',
                   'forwarded_from': 'category',
                   'media_type': 'category',
                   'duration_seconds': 'Int64',
                   'file': 'category'}

CHAT_RECORDS = {'id': 'category',
                'name': 'category',
                'type': 'category'}

CHAT_RECORDS_PREFIX = 'chat_'

STAT_FUNCTIONS = {f'{CHAT_RECORDS_PREFIX}id': 'nunique',
                  'id': 'count',
                  'text': lambda x: np.sum(x.str.len()),
                  'duration_seconds': 'sum'}

STAT_COLUMNS_NAMES = ['chats', 'msg', 'chr', 'media_sec']


def load_needed_chats_data(json_path: str) -> list:
    """
    Iteratively loads chats data from telegram takeout json file.
    This could be replaced with json.load(), if you don't want to use ijson.

    :param json_path: path to telegram takeout json file
    :return: list of dictionaries with chats data
    """
    with open(json_path, encoding='utf-8') as result_file:
        objects = ijson.items(result_file, 'chats.list.item')
        chats = (o for o in objects if o['type'] in CHAT_TYPES)
        res = []
        for chat in chats:
            c = {k: v for k, v in chat.items() if k in CHAT_RECORDS}
            c['messages'] = [{k: v for k, v in m.items() if k in MESSAGE_RECORDS} for m in chat['messages']]
            res.append(c)
        return res

def load_my_id(json_path: str) -> int:
    with open(json_path, encoding='utf-8') as result_file:
        objects = ijson.items(result_file, 'personal_information')
        return next(objects)['user_id']


def normalize_text_value(value: str | list,
                         replace_links=True) -> str:
    """
    In TG takeout the majority of message->text values are just strings,
    but some of them are lists of elements with 'type' field (like links, bold, etc.).
    So for more accurate statistics we need to normalize them to strings and replace links with placeholders.

    :param value: a string or a list of strings or dicts with 'type' field
    :param replace_links: replace links with placeholders
    :return: a normalized string
    """

    def elem_f(e): return f"<<link>>" if (replace_links and e['type'] == 'link') else e['text']

    if isinstance(value, list):
        return ' '.join([t if isinstance(t, str) else elem_f(t) for t in value])
    return value


def gen_messages_dataframe(chats_list,
                           chat_types=None,
                           print_stats=False) -> pd.DataFrame:
    """
    Generate dataframe from telegram takeout json data->chats->list.

    :param chats_list: list of chats
    :param print_stats: print to console dataframe info and memory usage
    :param chat_types: list of chat types to include in the resulting dataframe
    {'private_group', 'bot_chat', 'public_supergroup', 'saved_messages',
     'private_supergroup', 'private_channel', 'public_channel', 'personal_chat'}
    :return: dataframe with messages
    """

    if chat_types is None:
        chat_types = CHAT_TYPES

    df = pd.json_normalize(chats_list,
                           record_path="messages",
                           meta=["name", "id", "type"],
                           meta_prefix=CHAT_RECORDS_PREFIX,
                           errors="ignore")

    # leaving only selected columns
    chat_columns_types = {f'{CHAT_RECORDS_PREFIX}{k}': v for k, v in CHAT_RECORDS.items()}
    columns_types = {**chat_columns_types, **MESSAGE_RECORDS}
    df = df.reindex(list(columns_types.keys()), axis='columns')

    df = df[df[f'{CHAT_RECORDS_PREFIX}type'].isin(chat_types)]

    # removing service messages and other non-message records
    df = df[df['type'] == 'message']

    df['text'] = df['text'].apply(normalize_text_value)

    # removing 'user' prefix from 'from_id' fields
    df['from_id'] = df['from_id'].str.replace('user', '')
    df['from_id'] = df['from_id'].astype('uint64')

    # decreasing memory usage by converting to categorical when possible
    # df.convert_dtypes() is not working for categorical automatically
    df = df.astype(columns_types)

    if print_stats:
        print(df.head(15).to_string())
        print(df.info(verbose=False))
        print(df.memory_usage(deep=True))

    return df


def gen_stats_dataframe(df: pd.DataFrame,
                        exclude_forwarded=False,
                        freq="M") -> pd.DataFrame:
    """
    Generate dataframe with stats aggregated from dataframe with messages
    """
    stat_df = df
    if exclude_forwarded:
        stat_df = stat_df[stat_df['forwarded_from'].isna()]

    stat_df = stat_df.groupby(stat_df.date.dt.to_period(freq)).agg(STAT_FUNCTIONS)
    stat_df.columns = STAT_COLUMNS_NAMES
    return stat_df


def gen_sent_received_dataframe(msg_df: pd.DataFrame, my_id: int) -> pd.DataFrame:
    """
    Generate dataframe with separated stats for sent and received messages.
    :param msg_df: dataframe with messages
    :param my_id: owner id number
    :return: dataframe with stats
    """
    sent_df = gen_stats_dataframe(msg_df[msg_df['from_id'] == my_id], exclude_forwarded=True)
    received_df = gen_stats_dataframe(msg_df[msg_df['from_id'] != my_id])
    return sent_df.merge(received_df, on='date', suffixes=('_sent', '_received'))


def gen_stat_plotly_fig(msg_df: pd.DataFrame,
                        my_id: int,
                        start_date=pd.Timestamp.min,
                        end_date=pd.Timestamp.max,
                        show_received=False) -> go.Figure:
    """
    Generate plotly figure from dataframe with messages.
    :param msg_df: dataframe with messages
    :param my_id: my id number
    :param start_date: start date for plotting
    :param end_date: end date for plotting
    :param show_received: show received messages stats together with sent
    """
    merged_df = gen_sent_received_dataframe(msg_df[(start_date <= msg_df['date']) &
                                                   (msg_df['date'] <= end_date)], my_id)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=merged_df.index.to_timestamp(), y=merged_df['chr_sent'], name='chr_sent'), secondary_y=False)
    if show_received:
        fig.add_trace(go.Bar(x=merged_df.index.to_timestamp(), y=merged_df['chr_received'], name='chr_received'),
                      secondary_y=False)

    fig.add_trace(go.Scatter(x=merged_df.index.to_timestamp(), y=merged_df['msg_sent'], name='msg'), secondary_y=True)

    fig.update_layout(title="TG sent messages stats",
                      yaxis_title="characters sent",
                      font={"size": 20, "color": "black"})
    fig.update_yaxes(title_text="messages sent", secondary_y=True)
    return fig


def main(path: str) -> None:
    jdata = load_needed_chats_data(path)
    my_id = load_my_id(path)

    df = gen_messages_dataframe(jdata, print_stats=True)
    gen_stat_plotly_fig(df, my_id,
                        start_date=pd.Timestamp('2021-01-01'),
                        end_date=pd.Timestamp('2023-12-31'),
                        show_received=True).show()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 tg-takeout-parser.py <result.json>")
        sys.exit()

    main(sys.argv[1])
