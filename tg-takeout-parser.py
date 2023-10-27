# author: @ulyantsev

import json
import sys

import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


MESSAGE_RECORDS = {'id': 'int',
                   'date': 'datetime64[ns]',
                   'type': 'category',
                   'from_id': 'category',
                   'text': 'string',
                   'forwarded_from': 'category',
                   'media_type': 'category',
                   'file': 'category'}

CHAT_RECORDS = {'id': 'category',
                'name': 'category',
                'type': 'category'}

CHAT_RECORDS_PREFIX = 'chat_'

def load_takeout_json(filename: str) -> dict:
    pass

def normalize_text_value(t_value,
                         replace_links=True) -> str:
    """
    In TG takeout the majority of message->text values are just strings,
    but some of them are lists of elements with 'type' field (like links, bold, etc).
    So for more accurate statistics we need to normalize them to strings and replace links with placeholders
    :param t_value: a string or a list of strings or dicts with 'type' field
    :param replace_links: replace links with placeholders
    :return: a normalized string
    """

    def elem_f(e): return f"<<{e['type']}>>" if (replace_links and e['type'] == 'link') else e['text']

    if isinstance(t_value, list):
        return ' '.join([t if isinstance(t, str) else elem_f(t) for t in t_value])
    return t_value

def gen_messages_dataframe(jdata,
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
        chat_types = ['private_group', 'personal_chat']

    df = pd.json_normalize(jdata["chats"]["list"],
                           record_path="messages",
                           meta=["name", "id", "type"],
                           meta_prefix=CHAT_RECORDS_PREFIX,
                           errors="ignore")

    # leaving only selected columns
    chat_columns_types = {f'{CHAT_RECORDS_PREFIX}{k}': v for k, v in CHAT_RECORDS.items()}
    columns_types = {**chat_columns_types, **MESSAGE_RECORDS}
    df = df[list(columns_types.keys())]

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

    agg_functions = {f'{CHAT_RECORDS_PREFIX}id': 'nunique',
                     'id': 'count',
                     'text': lambda x: np.sum(x.str.len())}
    stat_df = stat_df.groupby(stat_df.date.dt.to_period(freq)).agg(agg_functions)
    stat_df.columns = ['chats', 'messages', 'chars']
    return stat_df


def gen_plotly_fig(df: pd.DataFrame) -> go.Figure:
    """
    Generate plotly figure from dataframe with messages
    :param df: dataframe with stats
    """


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 tg-takeout-parser.py <result.json>")
        sys.exit()

    with open(sys.argv[1], encoding='utf-8') as result_file:

        # this could be done with iterative parsing, but it's not needed for now
        jdata = json.load(result_file)

        my_id = jdata['personal_information']['user_id']
        print(f"my_id: {my_id}")

        df = gen_messages_dataframe(jdata)
        print(set(df['from_id']))

        sent_df = gen_stats_dataframe(df[df['from_id'] == my_id], freq='Q', exclude_forwarded=True)
        received_df = gen_stats_dataframe(df[df['from_id'] != my_id], freq='Q')

        print(sent_df.merge(received_df, on='date', suffixes=('_sent', '_received')).to_string())
