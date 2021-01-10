import pandas as pd
import numpy as np
import time
from redis_util import redis_server, getAID_in_one, getAID_in_two, getAIDPos
from mongo_readraw import mongoDBManager


class TimeRecord:
    def __init__(self, d_str):
        obj = pd.to_datetime(d_str, unit='ms')
        self.month = obj.year, obj.month
        self.week = obj.year, obj.week
        self.date = obj.year, obj.month, obj.day  # [Y, M, D]


class Popular_Rank_Inserter:

    def __init__(self):
        self.redis = redis_server

    def process_one_entry(self, lookup, timeKey, aid):
        if aid in lookup:
            pair = lookup[aid]
            if timeKey in pair:
                pair[timeKey] = pair[timeKey] + 1
            else:
                pair[timeKey] = 1
        else:
            pair = dict()
            pair[timeKey] = 1
            lookup[aid] = pair

    def groupArticlesWithTimestamp(self, read_iter):
        day_lookup_one, week_lookup_one, month_lookup_one, \
            day_lookup_two, week_lookup_two, month_lookup_two = \
            {}, {}, {}, {}, {}, {}
        aid_one, aid_two = getAID_in_one(), getAID_in_two()
        day_lookup_pos, week_lookup_pos, month_lookup_pos = \
            [day_lookup_one, day_lookup_two], \
            [week_lookup_one, week_lookup_two], \
            [month_lookup_one, month_lookup_two]
        for i, row in enumerate(read_iter):
            aid = row['aid']
            aid_bytes = aid.encode('utf-8')
            t = TimeRecord(row['timestamp'])
            if aid_bytes in aid_one:
                self.process_one_entry(week_lookup_one, t.week, aid)
                self.process_one_entry(month_lookup_one, t.month, aid)
                self.process_one_entry(day_lookup_one, t.date, aid)
            elif aid_bytes in aid_two:
                self.process_one_entry(week_lookup_two, t.week, aid)
                self.process_one_entry(month_lookup_two, t.month, aid)
                self.process_one_entry(day_lookup_two, t.date, aid)
            else:
                pos = getAIDPos(aid, mongoDBManager)
                self.process_one_entry(week_lookup_pos[pos], t.week, aid)
                self.process_one_entry(month_lookup_pos[pos], t.month, aid)
                self.process_one_entry(day_lookup_pos[pos], t.date, aid)
        return day_lookup_one, week_lookup_one, month_lookup_one, \
            day_lookup_two, week_lookup_two, month_lookup_two

    def reducePopularRank(self, lookup, limit):
        return sorted(lookup.items(), key=lambda v: max(v[1].values()), reverse=True)[:limit]

    def reduceTotalPopularRank(self, lookup):
        return sorted(lookup, key=lambda v: max(v[1].values()), reverse=True)[:5]

    def getPopularArticles(self, read_iter):
        dayList_res_one, weekList_res_one, monthList_res_one, \
            dayList_res_two, weekList_res_two, monthList_res_two = \
            self.groupArticlesWithTimestamp(read_iter)

        dayList_res_one = self.reducePopularRank(dayList_res_one, 5)
        weekList_res_one = self.reducePopularRank(weekList_res_one, 5)
        monthList_res_one = self.reducePopularRank(monthList_res_one, 5)

        dayList_res_two = self.reducePopularRank(dayList_res_two, 5)
        weekList_res_two = self.reducePopularRank(weekList_res_two, 5)
        monthList_res_two = self.reducePopularRank(monthList_res_two, 5)

        return dayList_res_one, weekList_res_one, monthList_res_one, \
            dayList_res_two, weekList_res_two, monthList_res_two

    def insertPopularArticles(self, read_iter, pop_single_col, pop_total_col):
        pop_single_col[0].drop()
        pop_single_col[1].drop()
        pop_total_col.drop()
        dayList_res_one, weekList_res_one, monthList_res_one, \
            dayList_res_two, weekList_res_two, monthList_res_two = \
            self.getPopularArticles(read_iter)
        pop_single_col[0].insert_one({'id': 1,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'daily',
                                      'articleAidList': [k for k, _ in dayList_res_one]
                                      })
        pop_single_col[0].insert_one({'id': 2,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'weekly',
                                      'articleAidList': [k for k, _ in weekList_res_one]
                                      })
        pop_single_col[0].insert_one({'id': 3,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'monthly',
                                      'articleAidList': [k for k, _ in monthList_res_one]
                                      })

        pop_single_col[1].insert_one({'id': 1,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'daily',
                                      'articleAidList': [k for k, _ in dayList_res_two]
                                      })
        pop_single_col[1].insert_one({'id': 2,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'weekly',
                                      'articleAidList': [k for k, _ in weekList_res_two]
                                      })
        pop_single_col[1].insert_one({'id': 3,
                                      'timestamp': time.time() * 1000,
                                      'temporalGranularity': 'monthly',
                                      'articleAidList': [k for k, _ in monthList_res_two]
                                      })

        dayList_res_final = self.reduceTotalPopularRank(
            dayList_res_one + dayList_res_two
        )
        weekList_res_final = self.reduceTotalPopularRank(
            weekList_res_one + weekList_res_two
        )
        monthList_res_final = self.reduceTotalPopularRank(
            monthList_res_one + monthList_res_two
        )

        pop_total_col.insert_one({'id': 1,
                                  'timestamp': time.time() * 1000,
                                  'temporalGranularity': 'daily',
                                  'articleAidList': [k for k, _ in dayList_res_final]
                                  })
        pop_total_col.insert_one({'id': 2,
                                  'timestamp': time.time() * 1000,
                                  'temporalGranularity': 'weekly',
                                  'articleAidList': [k for k, _ in weekList_res_final]
                                  })
        pop_total_col.insert_one({'id': 3,
                                  'timestamp': time.time() * 1000,
                                  'temporalGranularity': 'monthly',
                                  'articleAidList': [k for k, _ in monthList_res_final]
                                  })


rankInserter = Popular_Rank_Inserter()
