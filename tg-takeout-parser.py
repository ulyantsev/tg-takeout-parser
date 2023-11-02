# author: @ulyantsev

import json
import ijson
import sys
import time

import numpy as np
import pandas as pd

import plotly.express as px
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

def load_selected_msg_data(json_path: str):
    with open(json_path, encoding='utf-8') as result_file:
        result_file.seek(0)
        objects = ijson.items(result_file, 'chats.list.item')
        chats = (o for o in objects if o['type'] in CHAT_TYPES)
        res = []
        for chat in chats:
            c = {k: v for k, v in chat.items() if k in CHAT_RECORDS}
            c['messages'] = [{k: v for k, v in m.items() if k in MESSAGE_RECORDS} for m in chat['messages']]
            res.append(c)
        return res

def load_my_id(json_path: str):
    with open(json_path, encoding='utf-8') as result_file:
        objects = ijson.items(result_file, 'personal_information')
        return next(objects)['user_id']


def normalize_text_value(value,
                         replace_links=True) -> str:
    """
    In TG takeout the majority of message->text values are just strings,
    but some of them are lists of elements with 'type' field (like links, bold, etc.).
    So for more accurate statistics we need to normalize them to strings and replace links with placeholders
    :param value: a string or a list of strings or dicts with 'type' field
    :param replace_links: replace links with placeholders
    :return: a normalized string
    """

    def elem_f(e): return f"<<{e['type']}>>" if (replace_links and e['type'] == 'link') else e['text']

    if isinstance(value, list):
        return ' '.join([t if isinstance(t, str) else elem_f(t) for t in value])
    return value


def gen_messages_dataframe(chats_list,
                           chat_types=None,
                           print_stats=False) -> pd.DataFrame:
    """
    Generate dataframe from telegram takeout json data

    For my data the set of fields in "messages" is:
    ['id', 'type', 'date', 'date_unixtime', 'from', 'from_id', 'text',
       'text_entities', 'file', 'mime_type', 'thumbnail', 'media_type',
       'duration_seconds', 'width', 'height', 'forwarded_from', 'saved_from',
       'photo', 'sticker_emoji', 'edited', 'edited_unixtime',
       'reply_to_message_id', 'actor', 'actor_id', 'action', 'title',
       'contact_information.first_name', 'contact_information.last_name',
       'contact_information.phone_number', 'members', 'via_bot',
       'location_information.latitude', 'location_information.longitude',
       'place_name', 'address', 'message_id', 'poll.question', 'poll.closed',
       'poll.total_voters', 'poll.answers', 'live_location_period_seconds',
       'discard_reason', 'inviter', 'contact_vcard', 'performer',
       'self_destruct_period_seconds', 'schedule_date', 'duration', 'to',
       'to_id', 'distance', 'reason_domain', 'game_title', 'game_description',
       'game_link', 'game_message_id', 'score', 'chat_name', 'chat_id',
       'chat_type']
    But only several selected for the resulting dataframe.

    :param jdata:
    :param chat_meta_prefix:
    :param chat_types: list of chat types to include in the resulting dataframe
    {'private_group', 'bot_chat', 'public_supergroup', 'saved_messages',
     'private_supergroup', 'private_channel', 'public_channel', 'personal_chat'}
    :return:
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
    df = df.reindex(list(columns_types.keys()))

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


def gen_stat_plotly_fig(df: pd.DataFrame) -> go.Figure:
    """
    Generate plotly figure from dataframe with messages
    :param df: dataframe with stats
    """
    # sent_df = gen_stats_dataframe(df[df['from_id'] == my_id], exclude_forwarded=True)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # fig.add_trace(go.Bar(x=merged_df.index.to_timestamp(), y=merged_df['chr_sent'], name='chr'), secondary_y=False)
    # fig.add_trace(go.Bar(x=merged_df.index.to_timestamp(), y=merged_df['len_received'], name='received'),
    #               secondary_y=False)
    # fig.add_trace(go.Scatter(x=merged_df.index.to_timestamp(), y=merged_df['msg_sent'], name='msg'), secondary_y=True)
    return fig


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 tg-takeout-parser.py <result.json>")
        sys.exit()

    start_time = time.time()

    # with open(sys.argv[1], encoding='utf-8') as result_file:
    #     jdata = json.load(result_file)
    with open('x.json', 'w', encoding='utf-8') as f:
        f.write(str(load_selected_msg_data(sys.argv[1])))
        print(load_my_id(sys.argv[1]))


    print("--- %s seconds ---" % (time.time() - start_time))

    # with open(sys.argv[1], encoding='utf-8') as result_file:
    #
    #     # this could be done with iterative parsing, but it's not needed for now
    #     jdata = json.load(result_file)
    #
    #     my_id = jdata['personal_information']['user_id']
    #
    #     df = gen_messages_dataframe(jdata)
    #     print(df.head(15).to_string())
    #
    #     sent_df = gen_stats_dataframe(df[df['from_id'] == my_id], exclude_forwarded=True)
    #     received_df = gen_stats_dataframe(df[df['from_id'] != my_id])
    #
    #     merged_df = sent_df.merge(received_df, on='date', suffixes=('_sent', '_received'))
    #     print(merged_df)



