# -*- coding: utf-8 -*-
import sys
import os
import yaml
from sqlalchemy import Table,literal_column,select,literal
import csv

def importVolumes(connection,metadata,sourcePath):

    invVolumes = Table('invVolumes',metadata)
    invTypes = Table('invTypes',metadata)
    trans = connection.begin()
    with open(os.path.join(sourcePath,'invVolumes1.csv'), 'r') as groupVolumes:
        volumereader=csv.reader(groupVolumes, delimiter=',')
        for group in volumereader:
            # SQLAlchemy 2.0: select() no longer takes a list, pass columns as separate arguments
            connection.execute(invVolumes.insert().from_select(['typeID','volume'],select(invTypes.c.typeID,literal(int(group[0]))).where(invTypes.c.groupID == int(group[1]))))
    with open(os.path.join(sourcePath,'invVolumes2.csv'), 'r') as groupVolumes:
        volumereader=csv.reader(groupVolumes, delimiter=',')
        for group in volumereader:
            connection.execute(invVolumes.insert().values(typeID=int(group[1]),volume=int(group[0])))
    trans.commit()
