from pyspark import SparkConf, SparkContext
from event import Event
from random import randint
import datetime
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

tsv_file = sc.textFile("/Users/gvsi/Developer/bdp/assign/data/assign7/dataSet7.tsv")

events_count = sc.accumulator(0)
filtered_events_count = sc.accumulator(0)
session_count = sc.accumulator(0)
shower_count = sc.accumulator(0)
removed_shower_count = sc.accumulator(0)

unique_sessions = set()


def unique_sessions_map(line):
    splits = re.split(r'\t+', line)
    user_id = splits[0]
    referring_domain = splits[3]
    key = (user_id, referring_domain)
    timestamp = splits[4]

    # increment session accumulator if unique
    l = len(unique_sessions)
    unique_sessions.add(key)
    if l + 1 == len(unique_sessions):
        session_count.add(1)

    # calculate type and subtype
    full_type = splits[1]
    i = full_type.index(' ')
    type = full_type[:i]
    subtype = full_type[i + 1:]

    return key, Event(type, subtype, timestamp)

user_sessions = tsv_file.map(unique_sessions_map).groupByKey()


def duplicate_events_map(session):
    key = session[0]
    events = list(set(session[1]))
    events_count.add(len(events))
    sorted_events = sorted(events, key=lambda event: (datetime.datetime.strptime(event.event_timestamp, "%Y-%m-%d %H:%M:%S.%f"), event.event_type, event.event_subtype))
    return key, sorted_events

user_sessions_without_duplicate_events = user_sessions.map(duplicate_events_map)


def shower_filter(session):
    events = session[1]

    contact_form_subtype_counts = 0
    click_event_count = 0
    show_event_count = 0
    display_event_count = 0

    for event in events:
        type = event.event_type
        subtype = event.event_subtype
        if subtype == "contact form":
            contact_form_subtype_counts += 1
        if type == "click":
            click_event_count += 1
        elif type == "show":
            show_event_count += 1
        elif type == "display":
            display_event_count += 1
    if contact_form_subtype_counts == 0 and click_event_count == 0 and (show_event_count > 0 or display_event_count > 0):
        # is shower
        shower_count.add(1)
        if randint(1,10) == 1:
            filtered_events_count.add(len(events))
            return True
        else:
            removed_shower_count.add(1)
            return False
    filtered_events_count.add(len(events))
    return True

filtered_user_sessions = user_sessions_without_duplicate_events.filter(shower_filter)


def domain_partitioner(key):
    referring_domain = key[1]
    return hash(referring_domain) % 6

partitioned_user_sessions = filtered_user_sessions.partitionBy(6, domain_partitioner)


partitioned_user_sessions.map(lambda tuple: (tuple[0], map(lambda event: str(event), tuple[1]))).saveAsTextFile("/Users/gvsi/Downloads/t2")

print("Total number of unique events before filtering: " + str(events_count.value))
print("Total number of unique events after filtering: " + str(filtered_events_count.value))
print("Total number of sessions: " + str(session_count.value))
print("Total number of SHOWER sessions: " + str(shower_count.value))
print("Total number of filtered SHOWER sessions: " + str(removed_shower_count.value))