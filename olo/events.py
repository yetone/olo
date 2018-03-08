from blinker import Namespace


signal = Namespace().signal

before_update = signal('before_update')
after_update = signal('after_update')
after_insert = signal('after_insert')
after_delete = signal('after_delete')
