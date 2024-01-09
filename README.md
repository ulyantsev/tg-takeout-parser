
This is a simple one-file code to extract the dataframe with messages
from Telegram-Desktop-exported chat history result.json.
I tried to make it as simple as possible, though some effort done to memory usage.

The example of usage contained in the `main` function.

You are free to change the constants in the beginning of the file to your needs. 
For example, you can add more fields to MESSAGE_RECORDS to extract:

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

Related projects:
- [MasterScrat](https://github.com/MasterScrat/Chatistics/tree/master)