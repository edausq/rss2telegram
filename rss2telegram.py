# -*- coding: utf-8 -*-
"""rss2telegram"""

import os
import feedparser
import boto3
import telegram

TELEGRAM_BOT = telegram.Bot(token=os.environ['TELEGRAM_TOKEN'])
DYNAMODB = boto3.resource('dynamodb', region_name=os.environ['AWS_DYNAMODB_REGION'])
DYNAMODB_TABLE = DYNAMODB.Table(token=os.environ['AWS_DYNAMODB_TABLE'])

def lambda_handler():
    """Function called by lambda"""
    all_flux = get_all_flux(DYNAMODB_TABLE)
    for flux in all_flux:
        current_items = get_rss_entries(flux['fluxUrl'])
        previous_items = flux['fluxOldItems']
        new_items = set(current_items).difference(previous_items)
        items_sent = send_items_to_telegram(TELEGRAM_BOT, new_items, flux['telegramChats'])
        update_flux(DYNAMODB_TABLE, flux, items_sent)

def get_rss_entries(url):
    """Returns items id (list) from rss flux url"""
    feed = feedparser.parse(url)
    return [entry["id"] for entry in feed.get("entries")]

def get_all_flux(table):
    """Retrieve flux list from DynamoDB"""
    response = table.scan()
    return response['Items']

def update_flux(table, flux, new_items):
    """Add new items (list) into dynamodb flux"""
    table.update_item(
        Key={
            'fluxId': flux['fluxId'],
        },
        UpdateExpression="set fluxOldItems = list_append(fluxOldItems, :items)",
        ExpressionAttributeValues={
            ':items': new_items
        },
        ReturnValues="NONE"
    )

def send_items_to_telegram(bot, items, chats):
    """Send items (list) to telegram chats (list)"""
    items_sent = []
    for chat in chats:
        for item in items:
            bot.send_message(str(chat), item)
            items_sent.append(item)
    return items_sent

def get_telegram_status(bot):
    """Return last telegram messages"""
    updates = bot.get_updates()
    for update in updates:
        print(str(update.effective_chat.id) + ' ' + update.effective_message.text)