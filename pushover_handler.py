import requests
import os

def send_pushover_notification(title, message, config):
    app_token = os.getenv('PUSHOVER_APP_TOKEN')
    user_key = os.getenv('PUSHOVER_USER_KEY')
    url = 'https://api.pushover.net/1/messages.json'
    payload = {
        'token': app_token,
        'user': user_key,
        'title': title,
        'message': message
    }
    response = requests.post(url, data=payload)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print('Pushover error:', response.text)
        raise