import imp
import importlib
import queue
import threading
from wikidataintegrator import wdi_core
from wikidata_credentials import *


class EditQueue:
    def __init__(self, write_thread_count=6):
        self.integrator = []
        for n in range(0, write_thread_count):
            self.integrator.append({})
            self.integrator[n]['parent'] = imp.load_module(
                'integrator' + str(n), *imp.find_module('wikidataintegrator'))
            self.integrator[n]['login']  = importlib.import_module(self.integrator[n]['parent'].__name__ + '.wdi_login').\
                                                WDLogin(user=wikidata_username, pwd=wikidata_password)
            self.integrator[n]['core'] = importlib.import_module(
                self.integrator[n]['parent'].__name__ + '.wdi_core')

        self.editqueue = queue.Queue(maxsize=0)
        self.event = threading.Event()
        self.editors = [threading.Thread(target=self.do_edits, kwargs={'n': n, 'event': self.event}) \
                        for n in range(0, write_thread_count)]

        for editor in self.editors:
            editor.start()

    def do_edits(self, n, event):
        while True:
            try:
                task = self.editqueue.get(timeout=1)
            except queue.Empty:
                if self.event.isSet():
                    break
                else:
                    continue
            try:
                itemengine = self.integrator[n]['core'].WDItemEngine(
                    wd_item_id=task[0], data=task[1])
                if task[2] is not None:
                    itemengine.set_label(task[2])
                if task[3] is not None:
                    itemengine.set_description(task[3])
                print(itemengine.write(self.integrator[n]['login']))
            except Exception as e:
                print('Exception when trying to edit ' + task[0] +
                      '; skipping')
                print(e)
            self.editqueue.task_done()

    def post(self, wikidata_item, data, label, description):
        self.editqueue.put((wikidata_item, data, label, description))

    def done(self):
        self.event.set()
