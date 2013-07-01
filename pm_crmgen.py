#!/usr/bin/python
# -*- coding: utf-8 -*-

# pm_crmgen : Pacemaker crm-file generator
#
# Copyright (C) 2010 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

import os
import sys
import codecs
import re
import csv
from optparse import OptionParser
from xml.dom.minidom import getDOMImplementation

CODE_PLATFORM = 'utf-8'
CODE_OUTFILE = 'utf-8'

# コメント開始文字
COMMENT_CHAR = '#'
# 表ヘッダ列番号
TBLHDR_POS = 1

# 内部モード識別子(処理中の表を識別)
M_NODE        = 'Node'
M_PROPERTY    = 'Property'
M_RSCDEFAULTS = 'RscDefaults'
M_OPDEFAULTS  = 'OpDefaults'
M_RESOURCES   = 'Resources'
M_ATTRIBUTES  = 'Attributes'
M_PRIMITIVE   = 'Primitive'
M_LOCATION    = 'Location'
M_LOCEXPERT   = 'LocExpert'
M_COLOCATION  = 'Colocation'
M_ORDER       = 'Order'
M_FTOPO       = 'FTopo'
M_TICKET      = 'Ticket'
M_CONFIG      = 'Config'
# {表ヘッダ文字列: 内部モード識別子}
MODE_TBL = {
  'node':              M_NODE,
  'property':          M_PROPERTY,
  'rsc_defaults':      M_RSCDEFAULTS,
  'op_defaults':       M_OPDEFAULTS,
  'resources':         M_RESOURCES,
  'rsc_attributes':    M_ATTRIBUTES,
  'primitive':         M_PRIMITIVE,
  'location':          M_LOCATION,
  'location_expert':   M_LOCEXPERT,
  'colocation':        M_COLOCATION,
  'order':             M_ORDER,
  'fencing_topology':  M_FTOPO,
  'rsc_ticket':        M_TICKET,
  'additional_config': M_CONFIG
}
M_SKIP = 'skip'     # 次の表ヘッダまでスキップ

# M_PRIMITIVEのサブモード
PRIM_PROP = 'p'     # Prop(erty)
PRIM_ATTR = 'a'     # Attr(ibutes)
PRIM_OPER = 'o'     # Oper(ation)
PRIM_MODE = [PRIM_PROP,PRIM_ATTR,PRIM_OPER]

# 必須列名
RQCLM_TBL = {
  (M_NODE,None):           ['uname','ptype','name','value'],
  (M_PROPERTY,None):       ['name','value'],
  (M_RSCDEFAULTS,None):    ['name','value'],
  (M_OPDEFAULTS,None):     ['name','value'],
  (M_RESOURCES,None):      ['resourceitem','id'],
  (M_ATTRIBUTES,None):     ['id','type','name','value'],
  (M_PRIMITIVE,PRIM_PROP): ['id','class','provider','type'],
  (M_PRIMITIVE,PRIM_ATTR): ['type','name','value'],
  (M_PRIMITIVE,PRIM_OPER): ['type'],
  (M_LOCATION,None):       ['rsc'],
  (M_LOCEXPERT,None):      ['rsc','score','bool_op','attribute','op','value'],
  (M_COLOCATION,None):     ['rsc','with-rsc','score'],
  (M_ORDER,None):          ['first-rsc','then-rsc','score'],
  (M_FTOPO,None):          ['node','rsc','index'],
  (M_TICKET,None):         ['ticket','rsc'],
  (M_CONFIG,None):         ['config']
}
# 非必須列名
CLM_NODE = ['ntype']
CLM_LOCEXPERT = ['role','id_spec']
CLM_COLOCATION = ['rsc-role','with-rsc-role']
CLM_ORDER = ['first-action','then-action','symmetrical']
CLM_TICKET = ['role','loss-policy']

# 種別
RESOURCE_TYPE = ['primitive','group','clone','ms','master']
ATTRIBUTE_TYPE = ['params','meta']
NODE_ATTR_TYPE = ['attributes','utilization']
PRIM_ATTR_TYPE = ['params','meta','utilization','operations']
# unary_op
UNARY_OP = ['defined','not_defined']

# INFINITYを示す文字列（出力時に使用）
SCORE_INFINITY = 'INFINITY'
# pingd/diskd使用時に生成するcolocationのスコア値
SCORE_PD_COLOCATION = SCORE_INFINITY
# crmファイルに出力するコメント
COMMENT_TBL = {
  'node':         '### Cluster Node ###',
  'property':     '### Cluster Option ###',
  'rsc_defaults': '### Resource Defaults ###',
  'op_defaults':  '### Operations Defaults ###',
  'primitive':    '### Primitive Configuration ###',
  'group':        '### Group Configuration ###',
  'clone':        '### Clone Configuration ###',
  'ms':           '### Master/Slave Configuration ###',
  'location':     '### Resource Location ###',
  'colocation':   '### Resource Colocation ###',
  'order':        '### Resource Order ###',
  'ftopo':        '### Fencing Topology ###',
  'ticket':       '### Resource Ticket ###',
  'config':       '### Additional Config ###'
}

# エラー/警告が発生した場合、Trueに設定する
errflg = False
warnflg = False
# テンポラリ的に使用
errflg2 = False


class Crm:
  ATTR_STATE = '_s'
  ATTR_CREATED = '_c'
  ATTR_UPDATED = '_u'
  mode = None,None
  pcr = []; attrd = {}
  rr = None; pr = None
  lineno = 0

  def __init__(self):
    self.input = None
    self.output = sys.stdout
    self.add_colocation = True
    self.add_order = True
    if not self.optionParser():
      sys.exit(1)
    try:
      s = []
      for x in sys.argv:
        s.append(unicode(x,CODE_PLATFORM))
    except Exception,msg:
      log.innererr(u'コマンドライン文字列のUnicodeへの変換に失敗しました。',msg)
      log.quitmsg(1)
      sys.exit(1)
    log.info(u'実行コマンドライン [%s]'%' '.join(s))
    try:
      self.doc = getDOMImplementation().createDocument(None,'crm',None)
      self.root = self.doc.documentElement
    except Exception,msg:
      log.innererr(u'DOM文書オブジェクトの生成に失敗しました。',msg)
      log.quitmsg(1)
      sys.exit(1)

  '''
    オプション解析
    [引数]
      なし
    [戻り値]
      True  : OK
      False : NG（不正なオプションあり）
  '''
  def optionParser(self):
    usage = '%prog [options] CSV_FILE'
    version = '2.0'
    description = "  character encoding of supported CSV_FILE are 'UTF-8' and 'Shift_JIS'"
    prog = 'pm_crmgen'
    p = OptionParser(usage=usage,version=version,description=description,prog=prog)
    p.add_option('-o',dest='output_file',
      help='output generated crm-file to the named file (default: stdout)')
    p.add_option('-V',action='count',dest='loglevel',default=Log.ERROR,
      help='turn on debug info. additional instances increase verbosity')
    s = ' related to the pingd/diskd (in LOCATION table) is NOT generated'
    p.add_option('-C',action='store_false',dest='add_colocation',default=True,
      help='colocation constraint' + s)
    p.add_option('-O',action='store_false',dest='add_order',default=True,
      help='order constraint' + s)
    try:
      opts,args = p.parse_args()
    except SystemExit,retcode:
      if str(retcode) != '0':
        return False
      sys.exit(0)
    except Exception:
      log.stderr(u'オプションの解析に失敗しました。\n')
      return False
    if len(args) != 1:
      if len(args) == 0:
        log.stderr(u'CSVファイルが指定されていません。\n\n')
      else:
        log.stderr(u'CSVファイルが複数指定されています。\n\n')
      p.print_help(sys.stderr)
      return False
    try:
      self.input = unicode(args[0],CODE_PLATFORM)
      log.printitem_file = os.path.basename(self.input)
      if opts.output_file:
        self.output = unicode(opts.output_file,CODE_PLATFORM)
    except Exception:
      log.stderr(u'ファイル名のUnicodeへの変換に失敗しました。\n')
      return False
    log.level = opts.loglevel
    self.add_colocation = opts.add_colocation
    self.add_order = opts.add_order
    return True

  def skip_mode(self,flg):
    if flg:
      self.mode = M_SKIP,self.mode[1]

  '''
    表ヘッダ解析
    [引数]
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG
  '''
  def analyze_header_tbl(self,csvl):
    data = csvl[TBLHDR_POS].lower()
    if self.mode[0] == M_PRIMITIVE and data in PRIM_MODE:
      if (not self.mode[1] and data != PRIM_PROP or
              self.mode[1] and data == PRIM_PROP):
        log.fmterr_l(u'Primitiveリソース表の定義が正しくありません。')
        self.skip_mode(True)
        return True
      self.mode = self.mode[0],data
      log.debug_l(u'サブモードを[%s]にセットしました。'%self.mode[1])
      if self.mode[1] == PRIM_ATTR:
        self.attrd = {}
      return True
    elif self.mode[0] == M_SKIP and data in PRIM_MODE:
      log.debug_l(u'エラー検知中のためサブモード[%s]はスキップします。'%data)
      return True
    x = MODE_TBL.get(data)
    if not x:
      log.fmterr_l(u'未定義の表ヘッダ [%s](%s) が設定されています。'
                   %(csvl[TBLHDR_POS],pos2clm(TBLHDR_POS)))
      return False
    self.mode = x,None
    log.debug_l(u'処理モードを[%s]にセットしました。'%self.mode[0])
    self.pcr = []; self.attrd = {}; self.pr = None
    return True

  '''
    列ヘッダ解析
    [引数]
      csvl : CSVファイル1行分のリスト
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      RIl  : resourceItem列（番号）を保持するリスト
    [戻り値]
      True  : OK
      False : NG
  '''
  def analyze_header_clm(self,csvl,clmd,RIl):
    ITEM_RI = 'resourceitem'
    def is_RI(clm):
      return (self.mode[0] == M_RESOURCES and clm == ITEM_RI)
    def get_location_clm(clm):
      x = clm.split(':')
      if clm.lower().startswith('score:') and len(x) == 2 and x[1]:
        return 'score:%s'%self.score_validate(x[1])
      elif clm.lower().startswith('pingd:') and len(x) == 3 and x[1] and x[2]:
        return 'pingd:%s:%s'%(x[1],x[2])
      elif clm.lower().startswith('diskd:') and len(x) == 2 and x[1]:
        return 'diskd:%s'%x[1]
    def output_msg(k,x,lpc,start,msgno):
      while range(lpc):
        i = clml.index(k,start)
        if msgno == 1:
          log.fmterr_l(
            u"'%s'列が複数設定されています。(%sと%s)"%(k,pos2clm(x),pos2clm(i)))
        elif msgno == 2:
          s = u'未定義の列 [%s](%s) が設定されています。'%(csvl[i],pos2clm(i))
          if self.mode[0] == M_LOCATION:
            log.fmterr_l(s)
          else:
            log.warn_l(s)
        start = i+1
        lpc -= 1
    global errflg2; errflg2 = False
    if self.mode == (M_PRIMITIVE,None):
      log.fmterr_l(u'Primitiveリソース表の定義が正しくありません。')
      self.skip_mode(True)
      return True
    clml = csvl[:]
    rql = RQCLM_TBL[self.mode][:]
    for i,data in [(i,x) for (i,x) in enumerate(csvl) if i > TBLHDR_POS and x]:
      clm = data.lower()
      if is_RI(clm):
        RIl.append(i)
      elif self.mode[1] == PRIM_OPER and clm not in RQCLM_TBL[self.mode]:
        rql.append(data)
        clm = data
      elif self.mode[0] == M_LOCATION and clm not in RQCLM_TBL[self.mode]:
        x = get_location_clm(data)
        if x:
          rql.append(x)
          clm = x
      elif (self.mode[0] == M_NODE and clm in CLM_NODE or
            self.mode[0] == M_LOCEXPERT and clm in CLM_LOCEXPERT or
            self.mode[0] == M_COLOCATION and clm in CLM_COLOCATION or
            self.mode[0] == M_ORDER and clm in CLM_ORDER or
            self.mode[0] == M_TICKET and clm in CLM_TICKET):
        rql.append(clm)
      if clm not in clmd:
        clmd[clm] = i
      clml[i] = clm
    for x in [x for x in rql if x not in clmd]:
      log.fmterr_l(u"'%s'列が設定されていません。"%x)
    l = dict2list(clmd)
    for k,x,cnt in [(k,x,clml.count(k)) for (k,x) in l if clml.count(k) > 1]:
      if k in rql and not is_RI(k):
        output_msg(k,x,cnt-1,x+1,1)
    for k,x,cnt in [(k,x,clml.count(k)) for (k,x) in l if k not in rql]:
      output_msg(k,x,cnt,x,2)
      del clmd[k]  # 不要な列
    for x in [x for (k,x) in dict2list(clmd) if has_non_ascii(k)]:
      log.warn_l(u'列定義に全角文字が含まれています。[%s](%s)'%(csvl[x],pos2clm(x)))
    if self.mode[0] == M_RESOURCES:
      if errflg2:
        return False
      if RIl[0] < clmd['id'] < RIl[len(RIl)-1]:
        log.fmterr_l(u'リソース構成表の定義が正しくありません。')
        return False
      del clmd[ITEM_RI]
    self.skip_mode(errflg2)
    return True

  '''
    有効データの有無チェック
    [引数]
      csvl : CSVファイル1行分のリスト
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      RIl  : resourceItem列（番号）を保持するリスト
    [戻り値]
      True  : 有効なデータあり
      False : 有効なデータなし
  '''
  def line_validate(self,csvl,clmd=None,RIl=None):
    def has_non_ascii_data(pos):
      for x in [x for x in pos if has_non_ascii(csvl[x])]:
        log.warn_l(
          u'設定値に全角文字が含まれています。[%s](%s)'%(csvl[x],pos2clm(x)))
    if clmd:
      # 実データの列数が列ヘッダのそれより少ない場合
      while range((max(clmd.values())+1) - len(csvl)):
        csvl.append('')  # 不足分
      if [x for x in clmd.values() if csvl[x]] or [x for x in RIl if csvl[x]]:
        has_non_ascii_data(RIl)
        has_non_ascii_data(dict2list(clmd,True))
        return True
      log.debug_l(u'実データが設定されていません。')
      return False
    else:
      if not csvl:
        log.debug_l(u'改行のみの行です。')
        return False
      elif csvl[0]:
        if csvl[0].startswith(COMMENT_CHAR):
          log.debug_l(u'コメントの行です。')
          return False
        log.debug_l(u'列Aに#以外から始まる文字列が設定されています。')
      if len(csvl) == csvl.count('') or len(csvl) <= TBLHDR_POS:
        log.debug_l(u'データなし行です。')
        return False
      return True

  def bool_validate(self,bool,pos):
    if not bool:
      return
    if bool.lower() in ['yes','y','true']:
      return 'true'
    elif bool.lower() in ['no','n','false']:
      return 'false'
    log.warn_l(u'無効な値 [%s](%s) が設定されています。'%(bool,pos2clm(pos)))

  def score_validate(self,score):
    if not score:
      return
    x = score.lower()
    if re.match('^[+-]?(inf|infinity)$',x) is not None:
      return x.replace('infinity',SCORE_INFINITY).replace('inf',SCORE_INFINITY)
    return score

  '''
    crmファイル生成（【CSV】->【XML】->【crmコマンド】）
    1.【CSV】データを「全て」読み込んで、【XML】形式にして保持
    2.【XML】から【crmコマンド】を生成し、出力
    [引数]
      なし
    [戻り値]
      0 : 正常終了
      1 : エラー発生
      2 : 警告発生
  '''
  def generate(self):
    log.debug_f(u'crmファイル生成処理を開始します。')
    log.debug_f(u'[ CSV -> XML ]処理を開始します。')
    try:
      fd = open(self.input,'rU')
    except Exception,msg:
      log.error(u'ファイルのオープンに失敗しました。[%s]'%self.input)
      log.error(msg)
      return 1
    try:
      code_infile = detect_char(fd.read())
      if not code_infile:
        fd.close()
        return 1
      fd.seek(0)
      csvReader = csv.reader(fd)
    except Exception,msg:
      log.error(u'ファイルの読み込みに失敗しました。[%s]'%self.input)
      log.error(msg)
      return 1
    while True:
      try:
        self.lineno = log.printitem_lineno = self.lineno + 1
        csvlr = csvReader.next()
        csvl = csvlr[:]
      except StopIteration:
        break  # 終端
      except Exception,msg:
        log.error(u'ファイルの読み込みに失敗しました。[%s]'%self.input)
        log.error(msg)
        return 1
      if (not unicode_listitem(csvl,code_infile,True) or
          not unicode_listitem(csvlr,code_infile,False)):
        fd.close()
        return 1
      if not self.line_validate(csvl):
        continue
      #
      # 表ヘッダ解析
      #
      if csvl[TBLHDR_POS]:
        log.debug_l(u'表ヘッダ解析処理を開始します。')
        if not self.analyze_header_tbl(csvl):
          break
        clmd = {}; RIl = []
        if not self.mode[1]:
          continue
      if self.mode[0] == M_SKIP:
        log.debug_l(u'次の表ヘッダ行までスキップします。')
        continue
      #
      # 列ヘッダ解析
      #
      if self.mode[0] and not clmd:
        log.debug_l(u'列ヘッダ解析処理を開始します。')
        if not self.analyze_header_clm(csvl,clmd,RIl):
          break
        continue
      #
      # 実データ解析
      #
      if not self.mode[0]:
        log.fmterr_l(u'表ヘッダが設定されていません。')
        fd.close()
        return 1
      if not self.line_validate(csvl,clmd,RIl):
        continue
      if not self.csv2xml(clmd,RIl,csvl,csvlr):
        break
    if not errflg:
      self.xml_check_resources()
    if errflg:
      fd.close()
      self.xml_debug()
      return 1
    try:
      fd.close()
    except Exception,msg:
      log.error(u'ファイルのクローズに失敗しました。[%s]'%self.input)
      log.error(msg)
      return 1
    if self.root.hasChildNodes():
      self.xml_debug()
      log.debug_f(u'[ XML -> crmコマンド ]処理を開始します。')
      if not self.write(self.xml2crm()):
        return 1
    else:
      log.warn(u'CSVファイルが不正です。(有効なデータが設定されていません。)')
    log.debug_f(u'crmファイル生成処理を終了します。')
    if warnflg:
      return 2
    return 0

  def csv2xml(self,clmd,RIl,csvl,csvlr):
    if self.mode[0] == M_NODE:
      log.debug_l(u'クラスタ・ノード表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.skip_mode(not self.csv2xml_node(clmd,csvl))
    elif self.mode[0] == M_PROPERTY:
      log.debug_l(u'クラスタ・プロパティ表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_option('property',clmd,csvl)
    elif self.mode[0] == M_RSCDEFAULTS:
      log.debug_l(u'リソース・デフォルト表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_option('rsc_defaults',clmd,csvl)
    elif self.mode[0] == M_OPDEFAULTS:
      log.debug_l(u'オペレーション・デフォルト表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_option('op_defaults',clmd,csvl)
    elif self.mode[0] == M_RESOURCES:
      log.debug_l(u'リソース構成表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      return self.csv2xml_resources(clmd,RIl,csvl)
    elif self.mode[0] == M_ATTRIBUTES:
      log.debug_l(u'リソース構成パラメータ表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.skip_mode(not self.csv2xml_attributes(clmd,csvl))
    elif self.mode[0] == M_PRIMITIVE:
      log.debug_l(u'Primitiveリソース表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.skip_mode(not self.csv2xml_primitive(clmd,csvl))
    elif self.mode[0] == M_LOCATION:
      log.debug_l(u'リソース配置制約表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_location(clmd,csvl)
    elif self.mode[0] == M_LOCEXPERT:
      log.debug_l(u'リソース配置制約（エキスパート）表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.skip_mode(not self.csv2xml_locexpert(clmd,csvl))
    elif self.mode[0] == M_COLOCATION:
      log.debug_l(u'リソース同居制約表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_colocation(clmd,csvl)
    elif self.mode[0] == M_ORDER:
      log.debug_l(u'リソース起動順序制約表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_order(clmd,csvl)
    elif self.mode[0] == M_FTOPO:
      log.debug_l(u'STONITHの実行順序表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_ftopo(clmd,csvl)
    elif self.mode[0] == M_TICKET:
      log.debug_l(u'リソースチケット制約表のデータを処理します。')
      self.debug_input(clmd,RIl,csvl)
      self.csv2xml_ticket(clmd,csvl)
    elif self.mode[0] == M_CONFIG:
      log.debug_l(u'追加設定表のデータを処理します。')
      self.debug_input(clmd,RIl,csvlr)
      self.csv2xml_config(clmd,csvlr)
    return True

  '''
    クラスタ・ノード表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_node(self,clmd,csvl):
    global errflg2; errflg2 = False
    changed = False
    uname = csvl[clmd['uname']]
    if uname:
      self.attrd = {}
      self.attrd['uname'] = uname; changed = True
    else:
      uname = self.attrd.get('uname')
      if not uname:
        log.fmterr_l(u"'uname'列に値が設定されていません。")
    ntype = ''
    if 'ntype' in clmd:
      ntype = csvl[clmd['ntype']]
      if ntype and changed:
        self.attrd['ntype'] = ntype
      elif ntype and not changed and uname:
        log.warn_l(u"'ntype'列に値が設定されています。")
        ntype = self.attrd.get('ntype','')
      elif not ntype and not changed:
        ntype = self.attrd.get('ntype','')
    ptype = csvl[clmd['ptype']].lower()
    if ptype and ptype not in NODE_ATTR_TYPE:
      log.fmterr_l(u'未定義のパラメータ種別 [ptype: %s] が設定されています。'
                   %csvl[clmd['ptype']])
    if not ptype:
      ptype = self.attrd.get('ptype','')
    name = csvl[clmd['name']]
    value = csvl[clmd['value']]
    if not ptype and (name or value):
      log.fmterr_l(u"'ptype'列に値が設定されていません。")
    self.attrd['ptype'] = ptype
    x = self.xml_get_node(self.root,'nodes')
    for node in self.xml_get_nodes(x,'node','uname',uname):
      if node.getAttribute('ntype') == ntype:
        break
    else:
      node = None
    self.xml_check_nv(node,ptype,name,value)
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <nodes>
    #     <node uname="pm01" ntype="normal">
    #       <attributes>
    #         <nv name="standby" value="off"/>
    #          :
    #       <utilization>
    #         <nv name="capacity" value="1"/>
    #          :
    #
    if not node:
      node = self.xml_create_child(x,'node')
      node.setAttribute('uname',uname)
      if ntype:
        node.setAttribute('ntype',ntype)
    if ptype:
      self.xml_append_nv(self.xml_get_node(node,ptype),name,value)
    return True

  '''
    クラスタ・プロパティ/リソース、オペレーション・デフォルト表データのXML化
    [引数]
      tag  : データ（<nv .../>）を追加するNodeのタグ名
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_option(self,tag,clmd,csvl):
    global errflg2; errflg2 = False
    name = csvl[clmd['name']]
    value = csvl[clmd['value']]
    self.xml_check_nv(self.root,tag,name,value)
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <property>
    #     <nv name="no-quorum-policy" value="ignore"/>
    #      :
    #   <rsc_defaults>
    #     <nv name="resource-stickiness" value="INFINITY"/>
    #      :
    #   <op_defaults>
    #     <nv name="record-pending" value="true"/>
    #      :
    #
    return self.xml_append_nv(self.xml_get_node(self.root,tag),name,value)

  '''
    リソース構成表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      RIl  : resourceItem列（番号）を保持するリスト
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_resources(self,clmd,RIl,csvl):
    global errflg2; errflg2 = False
    pos = 0
    x = [x for x in RIl if csvl[x]]
    if len(x) == 0:
      log.fmterr_l(u"'resourceItem'列に値が設定されていません。")
    elif len(x) > 1:
      log.fmterr_l(u"複数の'resourceItem'列に値が設定されています。")
    elif csvl[x[0]].lower() not in RESOURCE_TYPE:
      log.fmterr_l(
        u'未定義のリソース種別 [type: %s] が設定されています。'%csvl[x[0]])
    else:
      pos = x[0]
      if csvl[pos].lower() == 'master':
        csvl[pos] = 'ms'
      ri = csvl[pos].lower()
    log.debug1_l('rsc_config: - %s'%self.pcr)
    depth = -1
    if pos > 0:
      if self.pcr:
        for i in [i for (i,x) in enumerate(RIl[:len(self.pcr)+1]) if x == pos]:
          depth = i
      elif csvl[RIl[0]]:
        depth = 0
    if pos > 0 and depth == -1:
      log.fmterr_l(u"'resourceItem'列 (リソース構成) の設定に誤りがあります。")
    elif depth > 0:
      p_rt,p_id = self.pcr[depth-1]
      # primitive - (doesn't contain a resource)
      # group     -  primitive
      # clone     - {primitive|group}
      # ms        - {primitive|group}
      if (p_rt == 'primitive' or p_rt == 'group' and ri != 'primitive' or
          p_rt in ['clone','ms'] and ri not in ['primitive','group']):
        log.fmterr_l(u"リソース種別 ('resourceItem'列) の設定に誤りがあります。")
    rscid = csvl[clmd['id']]
    if not rscid:
      log.fmterr_l(u"'id'列に値が設定されていません。")
    elif self.rr:
      for x in [x for x in self.rr.childNodes if x.getAttribute('id') == rscid]:
        log.fmterr_l(u'[id: %s] のリソースは既に設定されています。(%s行目)'
                     %(rscid,x.getAttribute(self.ATTR_CREATED)))
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <resources>
    #     <group id="grpPg">
    #       <rsc id="prmEx"/>
    #       <rsc id="prmFs"/>
    #       <rsc id="prmIp"/>
    #       <rsc id="prmPg"/>
    #     </group>
    #     <primitive id="prmEx"/>
    #     <primitive id="prmFs"/>
    #     <primitive id="prmIp"/>
    #     <primitive id="prmPg"/>
    #   </resources>
    #
    if not self.rr:
      self.rr = self.xml_create_child(self.root,'resources')
    # 「<primitive|group|clone|ms id="xxx"/>」を追加
    x = self.xml_create_child(self.rr,ri)
    x.setAttribute('id',rscid)
    x.setAttribute(self.ATTR_CREATED,str(self.lineno))
    del self.pcr[depth:]
    self.pcr.append((ri,rscid))
    log.debug1_l('rsc_config: + %s'%self.pcr)
    if depth == 0:
      return True
    # 親子関係である場合は「<group|clone|ms>」の子として、
    x = self.xml_get_nodes(self.rr,p_rt,'id',p_id)[0]
    # 「<rsc id="yyy"/>」を追加
    self.xml_create_child(x,'rsc').setAttribute('id',rscid)
    return True

  '''
    リソース構成パラメータ表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
      node : データ（<params>/<meta>/<utilization>/<operations>...）を追加するNode
             ※Primitiveリソース表の処理時に指定される
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_attributes(self,clmd,csvl,node=None):
    global errflg2; errflg2 = False
    changed = False
    if not node:
      rscid = csvl[clmd['id']]
      if rscid:
        self.attrd['id'] = rscid; changed = True
      else:
        rscid = self.attrd.get('id')
        if not rscid:
          log.fmterr_l(u"'id'列に値が設定されていません。")
      node = self.xml_get_rscnode(rscid)
      types = ATTRIBUTE_TYPE
    else:
      types = PRIM_ATTR_TYPE
    atype = csvl[clmd['type']].lower()
    if atype:
      if atype in types:
        self.attrd['type'] = atype
      else:
        log.fmterr_l(u'未定義のパラメータ種別 [type: %s] が設定されています。'
                     %csvl[clmd['type']])
    else:
      if changed or not self.attrd.get('type'):
        log.fmterr_l(u"'type'列に値が設定されていません。")
      else:
        atype = self.attrd['type']
    name = csvl[clmd['name']]
    value = csvl[clmd['value']]
    self.xml_check_nv(node,atype,name,value)
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <resources>
    #     <clone id="clnPingd" ...>
    #       <meta>
    #         <nv name="clone-max" value="2"/>
    #          :
    #     <primitive id="prmEx" ...>
    #       <params>
    #         <nv name="device" value="/dev/xvdb1"/>
    #          :
    #       <utilization>
    #         <nv name="capacity" value="1"/>
    #          :
    #       <operations>
    #         <nv name="$id" value="opEx"/>
    #          :
    #
    return self.xml_append_nv(self.xml_get_node(node,atype),name,value)

  '''
    Primitiveリソース表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_primitive(self,clmd,csvl):
    global errflg2; errflg2 = False
    if self.mode[1] == PRIM_PROP and not self.pr:
      rscid = csvl[clmd['id']]
      if rscid:
        self.pr = self.xml_get_rscnode(rscid,'primitive')
        if self.pr and self.pr.getAttribute(self.ATTR_UPDATED):
          log.fmterr_l(
            u'[id: %s] のPrimitiveリソース表は既に設定されています。(%s行目)'
            %(rscid,self.pr.getAttribute(self.ATTR_UPDATED)))
        elif self.pr:
          self.pr.setAttribute(self.ATTR_UPDATED,str(self.lineno))
      else:
        log.fmterr_l(u"'id'列に値が設定されていません。")
      if not csvl[clmd['type']]:
        log.fmterr_l(u"'type'列に値が設定されていません。")
      if not csvl[clmd['class']] and csvl[clmd['provider']]:
        log.fmterr_l(u"'class'列に値が設定されていません。")
    elif self.mode[1] == PRIM_PROP:
      log.fmterr_l(u'Primitiveリソース表の定義が正しくありません。')
    elif self.mode[1] != PRIM_PROP and not self.pr:
      log.fmterr_l(u"表に「リソースID」('id'列に値) が設定されていません。")
    elif self.mode[1] == PRIM_OPER:
      optype = csvl[clmd['type']]
      if not optype:
        log.fmterr_l(u"'type'列に値が設定されていません。")
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <resources>
    #     <primitive id="prmEx" class="ocf" provider="heartbeat" type="sfex">
    #       <params>     ...</params>
    #       <meta>       ...</meta>
    #       <utilization>...</utilization>
    #       <operations> ...</operations>
    #       <op>
    #         <start>
    #           <nv name="interval" value="0s"/>
    #            :
    #         <monitor>
    #          :
    #
    if self.mode[1] == PRIM_PROP:
      for x in [x for x in RQCLM_TBL[self.mode] if csvl[clmd[x]]]:
        self.pr.setAttribute(x,csvl[clmd[x]])
    elif self.mode[1] == PRIM_ATTR:
      return self.csv2xml_attributes(clmd,csvl,self.pr)
    elif self.mode[1] == PRIM_OPER:
      o = self.xml_create_child(self.xml_get_node(self.pr,'op'),optype)
      for k,x in clmd.items():
        if k in RQCLM_TBL[self.mode] or not csvl[x]:
          continue
        self.xml_append_nv(o,k,csvl[x])
    return True

  '''
    リソース配置制約表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_location(self,clmd,csvl):
    def set_attr(tup):
      if not self.xml_need2xml_constraint('location',tup):
        return False
      a = self.xml_create_child(tup[0],'rule')
      for k,x in tup[1].items():
        a.setAttribute(k,x)
      a.setAttribute(self.ATTR_CREATED,'%s %s'%(self.lineno,tup[2]))
      return True
    global errflg2; errflg2 = False
    rsc = csvl[clmd['rsc']]
    if rsc:
      self.xml_get_rscnode(rsc)
    else:
      log.fmterr_l(u"'rsc'列に値が設定されていません。")
    if errflg2:
      return True
    #
    # Example:
    # <crm>
    #   <locations>
    #     <location rsc="grpPg">
    #       <rule type="uname" score="200" node="pm01"/>
    #       <rule type="uname" score="100" node="pm02"/>
    #       <rule type="pingd" score="-INFINITY" attr="default_ping_set" value="100"/>
    #       <rule type="diskd" score="-INFINITY" attr="diskcheck_status"/>
    #       <rule type="diskd" score="-INFINITY" attr="diskcheck_status_internal"/>
    #     </location>
    #
    l = self.xml_get_nodes(self.root,'location','rsc',rsc)
    if l:
      l = l[0]
    else:
      l = self.doc.createElement('location')
      l.setAttribute('rsc',rsc)
    for k,x in dict2list(clmd):
      if k.startswith('score:') and csvl[x]:
        for node in csvl[x].split():
          attr = {'type':'uname','score':k.split(':')[1],'node':node}
          set_attr((l,attr,x))
      elif (k.startswith('pingd:') or k.startswith('diskd:')) and csvl[x]:
        if self.bool_validate(csvl[x],x) != 'true':
          continue
        k = k.split(':')
        attr = {'type':k[0],'score':'-%s'%SCORE_INFINITY,'attr':k[1]}
        if k[0] == 'pingd':
          attr['value'] = k[2]
        if (not set_attr((l,attr,x)) or
            not self.add_colocation and not self.add_order):
          continue
        # 関連するcolocation/orderを生成
        ids = self.xml_get_parentids(k[0],'params','name',k[1])
        if not ids:
          log.fmterr_l(
            u'パラメータ [(name=) %s] を設定した「%sリソース」が特定できません。'
            %(k[1],k[0]))
          continue
        for rscid in ids:
          if self.add_colocation:
            self.csv2xml_colocation(
                  {'score':0,
                   'rsc':1,
                   'with-rsc':2,
                   self.ATTR_STATE:3},
                  [SCORE_PD_COLOCATION,
                   rsc,
                   rscid,
                   'autogen'])
          if self.add_order:
            self.csv2xml_order(
                  {'score':0,
                   'first-rsc':1,
                   'then-rsc':2,
                   'symmetrical':3,
                   self.ATTR_STATE:4},
                  ['0',
                   rscid,
                   rsc,
                   'no',
                   'autogen'])
    if l.hasChildNodes():
      self.xml_get_node(self.root,'locations').appendChild(l)
    else:
      log.warn_l(u"配置制約対象のリソースID ('rsc'列) のみ設定されています。")
      l.unlink()
    return True

  '''
    リソース配置制約（エキスパート）表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_locexpert(self,clmd,csvl):
    def has_idspec(tgtref):
      if 'id_spec' in clmd:
        x = csvl[clmd['id_spec']]
        if not tgtref:
          return x != ''
        return x and x.startswith('$id-ref=') or x.startswith('id-ref=')
      return False
    def set_attr(node,names,tag=None,setflg=False):
      if tag: e = None
      else:   e = node
      for k,x in [(k,x) for (k,x) in clmd.items() if k in names and csvl[x]]:
        if not e:
          e = self.xml_create_child(node,tag)
        e.setAttribute(k,csvl[x])
        setflg = True
      if setflg:
        e.setAttribute(self.ATTR_CREATED,str(self.lineno))
    global errflg2; errflg2 = False
    changed = False; r = None
    rsc = csvl[clmd['rsc']]
    if rsc:
      self.attrd['rsc'] = rsc; changed = True
    else:
      rsc = self.attrd.get('rsc')
      if not rsc:
        log.fmterr_l(u"'rsc'列に値が設定されていません。")
    self.xml_get_rscnode(rsc)
    if (has_idspec(True) and
        not [k for (k,x) in clmd.items() if k not in ['rsc','id_spec'] and csvl[x]]):
      r = self.doc.createElement('rule')
    if not r:
      x = self.score_validate(csvl[clmd['score']])
      if x:
        csvl[clmd['score']] = self.attrd['score'] = x
        r = self.doc.createElement('rule')
      else:
        if changed or not rsc:
          log.fmterr_l(u"'score'列に値が設定されていません。")
        else:
          if csvl[clmd['bool_op']]:
            log.warn_l(u"'bool_op'列に値が設定されています。")
          if 'role' in clmd and csvl[clmd['role']]:
            log.warn_l(u"'role'列に値が設定されています。")
          if has_idspec(False):
            log.warn_l(u"'id_spec'列に値が設定されています。")
          csvl[clmd['score']] = self.attrd['score']
      if not csvl[clmd['attribute']]:
        log.fmterr_l(u"'attribute'列に値が設定されていません。")
      if not csvl[clmd['op']]:
        log.fmterr_l(u"'op'列に値が設定されていません。")
      else:
        if csvl[clmd['op']].lower() in UNARY_OP:
          if csvl[clmd['value']]:
            log.warn_l(u"'value'列に値が設定されています。")
        else:
          if not csvl[clmd['value']]:
            log.fmterr_l(u"'value'列に値が設定されていません。")
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <locexperts>
    #     <locexpert rsc="grpPg">
    #       <rule score="200" id_spec="$id=&quot;r1&quot;">
    #         <exp attribute="#uname" op="eq" value="pm01"/>
    #       </rule>
    #        :
    #       <rule score="-INFINITY" bool_op="or">
    #         <exp attribute="default_ping_set" op="not_defined"/>
    #         <exp attribute="default_ping_set" op="eq" value="yellow"/>
    #         <exp attribute="default_ping_set" op="eq" value="red"/>
    #       </rule>
    #       <rule score="-INFINITY" bool_op="and" role="Master">
    #         <exp attribute="attr1" op="ne" value="val1"/>
    #         <exp attribute="attr1" op="ne" value="val2"/>
    #       </rule>
    #     </locexpert>
    #     <locexpert rsc="grpPg2">
    #       <rule id_spec="$id-ref=&quot;r1&quot;"/>
    #        :
    #
    x = self.xml_get_node(self.root,'locexperts')
    l = self.xml_get_nodes(x,'locexpert','rsc',rsc)
    if l:
      l = l[0]
    else:
      l = self.doc.createElement('locexpert')
      l.setAttribute('rsc',rsc)
      x.appendChild(l)
    if r:
      x = self.xml_get_nodes(x,'rule',self.ATTR_STATE,'working')
      if x:
        x[0].removeAttribute(self.ATTR_STATE)
      r.setAttribute(self.ATTR_STATE,'working')
      set_attr(r,['score','bool_op','role','id_spec'])
      l.appendChild(r)
    x = self.xml_get_nodes(l,'rule',self.ATTR_STATE,'working')[0]
    if x.getElementsByTagName('exp') and not x.getAttribute('bool_op'):
      log.fmterr_l(u"'bool_op'列に値が設定されていません。(%s行目)"
                   %x.getAttribute(self.ATTR_CREATED))
      return False
    set_attr(x,['attribute','op','value'],'exp')
    return True

  '''
    リソース同居制約表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_colocation(self,clmd,csvl):
    global errflg2; errflg2 = False
    for x in [x for x in RQCLM_TBL[M_COLOCATION,None] if not csvl[clmd[x]]]:
      log.fmterr_l(u"'%s'列に値が設定されていません。"%x)
    self.xml_get_rscnode(csvl[clmd['rsc']])
    self.xml_get_rscnode(csvl[clmd['with-rsc']])
    x = self.score_validate(csvl[clmd['score']])
    if x:
      csvl[clmd['score']] = x
    if errflg2:
      return False
    if not self.xml_need2xml_constraint('colocation',(clmd,csvl)):
      return True
    #
    # Example:
    # <crm>
    #   <colocations>
    #     <colocation score="INFINITY" rsc="grpPg" with-rsc="clnPingd"/>
    #     <colocation score="INFINITY" rsc="grpPg" with-rsc="clnDiskd1"/>
    #     <colocation score="INFINITY" rsc="grpPg" with-rsc="clnDiskd2"/>
    #   </colocations>
    #
    c = self.xml_create_child(
          self.xml_get_node(self.root,'colocations'),'colocation')
    for k,x in [(k,x) for (k,x) in clmd.items() if csvl[x]]:
      c.setAttribute(k,csvl[x])
    c.setAttribute(self.ATTR_CREATED,str(self.lineno))
    return True

  '''
    リソース起動順序制約表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_order(self,clmd,csvl):
    global errflg2; errflg2 = False
    for x in [x for x in RQCLM_TBL[M_ORDER,None] if not csvl[clmd[x]]]:
      log.fmterr_l(u"'%s'列に値が設定されていません。"%x)
    self.xml_get_rscnode(csvl[clmd['first-rsc']])
    self.xml_get_rscnode(csvl[clmd['then-rsc']])
    x = self.score_validate(csvl[clmd['score']])
    if x:
      csvl[clmd['score']] = x
    if clmd.get('symmetrical'):
      x = self.bool_validate(csvl[clmd['symmetrical']],clmd['symmetrical'])
      if x:
        csvl[clmd['symmetrical']] = x
    if errflg2:
      return False
    if not self.xml_need2xml_constraint('order',(clmd,csvl)):
      return True
    #
    # Example:
    # <crm>
    #   <orders>
    #     <order score="0" first-rsc="clnPingd"  then-rsc="grpPg" symmetrical="false"/>
    #     <order score="0" first-rsc="clnDiskd1" then-rsc="grpPg" symmetrical="false"/>
    #     <order score="0" first-rsc="clnDiskd2" then-rsc="grpPg" symmetrical="false"/>
    #   </orders>
    #
    o = self.xml_create_child(self.xml_get_node(self.root,'orders'),'order')
    for k,x in [(k,x) for (k,x) in clmd.items() if csvl[x]]:
      o.setAttribute(k,csvl[x])
    o.setAttribute(self.ATTR_CREATED,str(self.lineno))
    return True

  '''
    STONITHの実行順序表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_ftopo(self,clmd,csvl):
    global errflg2; errflg2 = False
    node = csvl[clmd['node']]
    if node:
      self.attrd = {}
      self.attrd['node'] = node
    else:
      node = self.attrd.get('node')
    rsc = csvl[clmd['rsc']]
    if not rsc:
      log.fmterr_l(u"'rsc'列に値が設定されていません。")
    idx = csvl[clmd['index']]
    if not idx:
      log.fmterr_l(u"'index'列に値が設定されていません。")
    elif not idx.isdigit() or int(idx) < 1:
      log.fmterr_l(
        u'無効な値 [%s](%s) が設定されています。'%(idx,pos2clm(clmd['index'])))
    self.xml_get_rscnode(rsc,'primitive')
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <ftopos>
    #     <ftopo node="pm01" rsc="f1-1" index="1"/>
    #     <ftopo node="pm01" rsc="f1-2" index="1"/>
    #     <ftopo node="pm01" rsc="f1-3" index="2"/>
    #      :
    #   </ftopos>
    #
    f = self.xml_create_child(self.xml_get_node(self.root,'ftopos'),'ftopo')
    if node:
      f.setAttribute('node',node)
    f.setAttribute('rsc',rsc)
    f.setAttribute('index',idx)
    return True

  '''
    リソースチケット制約表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_ticket(self,clmd,csvl):
    global errflg2; errflg2 = False
    for x in [x for x in RQCLM_TBL[M_TICKET,None] if not csvl[clmd[x]]]:
      log.fmterr_l(u"'%s'列に値が設定されていません。"%x)
    self.xml_get_rscnode(csvl[clmd['rsc']])
    if errflg2:
      return False
    #
    # Example:
    # <crm>
    #   <tickets>
    #     <ticket id="ticketA" rsc="A" role="..." loss-policy="..."/>
    #   </tickets>
    #
    t = self.xml_create_child(self.xml_get_node(self.root,'tickets'),'ticket')
    for k,x in [(k,x) for (k,x) in clmd.items() if csvl[x]]:
      t.setAttribute(k,csvl[x])
    return True

  '''
    追加設定表データのXML化
    [引数]
      clmd : 列情報（[列名: 列番号]）を保持する辞書
      csvl : CSVファイル1行分のリスト
    [戻り値]
      True  : OK
      False : NG（フォーマット・エラー）
  '''
  def csv2xml_config(self,clmd,csvl):
    def del_r_blank(string):
      s = string.rstrip().rstrip(u'　')
      if string == s:
        return s
      return del_r_blank(s)
    l = csvl[clmd['config']].split('\n')
    for (i,x) in enumerate(l):
      l[i] = del_r_blank(del_r_blank(x).rstrip('\\'))
    #
    # Example:
    # <crm>
    #   <configs>
    #     <config data="..."/>
    #   </configs>
    #
    self.xml_create_child(self.xml_get_node(self.root,'configs'),'config'
      ).setAttribute('data',' \\\n'.join(l))
    return True

  '''
    制約データのXML化が必要かチェック（重複設定値の有無をチェック）
    ->配置制約表データ（pindg/diskd）から生成する同居・起動順序制約について、
      同じ制約は生成しない
    [引数]
      tag : {'location'|'colocation'|'order'}
      tup : tag == 'location': (node,attr,pos)
             - node : チェック対象<location>Node（下記※1）
             - attr : 設定値からXML化（<rule .../>）する属性名と値（辞書）
                      ->下記※2（既存）に対して同じ属性がないかチェック
             - pos  : 現在処理中のCSVの列番号
               ----
               <locations>
                 <location rsc="grpPg"> (※1)
                   <rule type="uname" score="200" node="pm01"/> (※2)
                   <rule type="uname" score="100" node="pm02"/> (※2)
                    :
                 </location>
               ----
            tag in ['colocation'|'order']: (clmd,csvl)
    [戻り値]
      True  : XML化の必要あり
      False : XML化の必要なし
  '''
  def xml_need2xml_constraint(self,tag,tup):
    if tag == 'location':
      for r in tup[0].childNodes:
        for k,x in tup[1].items():
          if r.getAttribute(k) != x:
            break
        else:
          lineno,pos = r.getAttribute(self.ATTR_CREATED).split()
          log.warn_l(u'%sの設定による制約は既に指定されています。(%s行目、%s)'
                     %(pos2clm(tup[2]),lineno,pos2clm(pos)))
          break
    elif tag in ['colocation','order']:
      if tag == 'colocation':
        clml = RQCLM_TBL[M_COLOCATION,None][:]
        l = CLM_COLOCATION
      elif tag == 'order':
        clml = RQCLM_TBL[M_ORDER,None][:]
        l = CLM_ORDER
      for x in l:
        clml.append(x)
      clmd = tup[0]; csvl = tup[1]
      for x in self.root.getElementsByTagName(tag):
        for k in clml:
          if k in clmd:
            data = csvl[clmd[k]]
          else:
            data = ''
          if x.getAttribute(k) != data:
            break
        else:
          if self.ATTR_STATE in clmd:
            return False
          elif x.getAttribute(self.ATTR_STATE):
            x.parentNode.removeChild(x).unlink()
          else:
            log.warn_l(u'同じ設定値が既に指定されています。(%s行目)'
                       %x.getAttribute(self.ATTR_CREATED))
          break
    return True

  def xml_get_node(self,node,tag):
    if node.getElementsByTagName(tag):
      return node.getElementsByTagName(tag)[0]
    return self.xml_create_child(node,tag)

  def xml_create_child(self,node,tag):
    x = self.doc.createElement(tag)
    node.appendChild(x)
    return x

  '''
    name列とvalue列の値のチェック
    -> <node ...><tag><nv name="xxx" value="yyy"/>と追加する前にチェック
    [引数]
      node  : 追加対象のNode
      tag   : 追加対象のtag（attribute種別 {params|meta}）
      name  : name列の値
      value : value列の値
    [戻り値]
      なし（結果はerrflg*を参照のこと）
  '''
  def xml_check_nv(self,node,tag,name,value):
    if not name and not value:
      return
    if not name:
      log.fmterr_l(u"'name'列に値が設定されていません。")
    if not value:
      log.info_l(u"'value'列に値が設定されていません。")
    if not node or not tag:
      return
    if not node.getElementsByTagName(tag):
      return
    x = node.getElementsByTagName(tag)[0].childNodes
    for x in [y for y in x if y.getAttribute('name') == name]:
      s = u''
      if node.getAttribute('id'):
        s = u'。%sの%sパラメータ'%(node.getAttribute('id'),tag)
      log.warn_l(u"項目 ('name'列%s) の値 [%s] は既に設定されています。(%s行目)"
                 %(s,name,x.getAttribute(self.ATTR_CREATED)))
      break

  def xml_append_nv(self,node,name,value):
    if not name and not value:
      return True
    x = self.xml_create_child(node,'nv')
    x.setAttribute('name',name)
    x.setAttribute('value',value)
    x.setAttribute(self.ATTR_CREATED,str(self.lineno))
    return True

  '''
    Node内から指定タグ名のNodeを取得
    [引数]
      node : 対象Node
      tags : 対象タグ名のリスト
    [戻り値]
      Nodeのリスト
  '''
  def xml_get_childs(self,node,tags):
    if node:
      return [x for x in node.childNodes if x.nodeName in tags]
    return []

  '''
    <resources>から指定id（+指定タグ名）のNodeを取得
    -> <resources><aaa id="xxx"/></resources>からidがxxxの<aaa>を取得
    [引数]
      id  : 対象リソースのid
      tag : 対象タグ名
    [戻り値]
      not None : 指定idのNode
          None : Node特定できず
                （-> 対象リソースがリソース構成表に設定されていない）
  '''
  def xml_get_rscnode(self,id,tag=None):
    if not id:
      return None
    if self.rr:
      for x in [x for x in self.rr.childNodes if x.getAttribute('id') == id]:
        if tag and x.nodeName != tag:
          continue
        return x
    if not tag:
      tag = ''
    log.fmterr_l(
      u'[id: %s] の%sリソースが「リソース構成表」に設定されていません。'%(id,tag))
    return None

  '''
    指定Nodeの全下位要素から指定タグ名かつ属性のNodeを取得
    [引数]
      node  : 対象Node
      tag   : 対象タグ名
      attr  : 対象属性名
      value : 対象属性値
    [戻り値]
      Nodeのリスト
  '''
  def xml_get_nodes(self,node,tag,attr,value):
    l = []
    if node:
      for x in node.getElementsByTagName(tag):
        if x.getAttribute(attr) == value:
          l.append(x)
    return l

  '''
    指定されたパラメータ(*)を持つPrimitiveリソースの最上位親リソースのidを取得
    (*) pingdの「name="name" value="default_ping_set"」など
    [引数]
      type  : <primitive type="aaa" ...>のtypeの値（{pingd|diskd}）
      tag   : <primitive ...><tag><nv .../>のtag（attribute種別 {params|meta}）
      name  : <primitive ...><tag><nv name="yyy" value="zzz"/>のnameの値
      value : <primitive ...><tag><nv name="yyy" value="zzz"/>のvalueの値
    [戻り値]
      idのリスト
  '''
  def xml_get_parentids(self,type,tag,name,value):
    # 親リソース（Node）のidを取得（親がいない場合は引数のidを返す）
    def xml_get_parentid(id):
      x = self.xml_get_nodes(self.rr,'rsc','id',id)
      if x:
        return xml_get_parentid(x[0].parentNode.getAttribute('id'))
      return id
    if not self.rr:
      return
    l = []
    for x in self.xml_get_nodes(self.rr,'primitive','type',type):
      for y in [y for z in x.getElementsByTagName(tag) for y in z.childNodes]:
        if y.getAttribute('name') == name and y.getAttribute('value') == value:
          rscid = xml_get_parentid(x.getAttribute('id'))
          if l.count(rscid) == 0:
            l.append(rscid)
          break
    return l

  '''
    リソース構成表データのチェック
      ・リソース構成表で設定したprimitiveリソースに対して
        Primitiveリソース表が設定されているか
      ・group/clone/msリソースにリソースが設定されているか
    [引数]
      なし
    [戻り値]
      なし（結果はerrflg*を参照のこと）
  '''
  def xml_check_resources(self):
    if not self.rr:
      return
    for x in self.rr.childNodes:
      rscid = x.getAttribute('id')
      if x.nodeName == 'primitive':
        if not x.getAttribute(self.ATTR_UPDATED):
          log.fmterr_f(
            u'[id: %s] の「Primitiveリソース表」が設定されていません。(%s行目)'
            %(rscid,x.getAttribute(self.ATTR_CREATED)))
      elif not self.xml_get_childs(x,['rsc']):
        log.fmterr_f(
          u'%sリソース [id: %s] にリソースが設定されていません。(%s行目)'
          %(x.nodeName,rscid,x.getAttribute(self.ATTR_CREATED)))

  '''
    XMLからcrmコマンド文字列を生成
    [引数]
      なし
    [戻り値]
      crmコマンド（群）文字列
  '''
  def xml2crm(self):
    s = [
      self.xml2crm_node(),
      self.xml2crm_option('property'),
      self.xml2crm_option('rsc_defaults'),
      self.xml2crm_option('op_defaults'),
      self.xml2crm_resources(['group','clone','ms']),
      self.xml2crm_primitive(),
      self.xml2crm_location(),
      self.xml2crm_colocation(),
      self.xml2crm_order(),
      self.xml2crm_ftopo(),
      self.xml2crm_ticket(),
      self.xml2crm_config()
    ]
    while [x for x in s if not x]:
      s.remove(None)
    return '\n'.join(s)

  def xml2crm_node(self):
    #
    # node <uname>[:<type>]
    #   [attributes  <param>=<value> [<param>=<value>...]]
    #   [utilization <param>=<value> [<param>=<value>...]]
    #
    s = []; tag = 'node'
    for x in self.root.getElementsByTagName(tag):
      y = []; z = []
      if x.getAttribute('ntype'):
        y.append(':%s'%x.getAttribute('ntype'))
      z.append('node %s%s'%(x.getAttribute('uname'),''.join(y)))
      self.xml2crm_attr(x,z,NODE_ATTR_TYPE)
      s.append(' \\\n\t'.join(z))
    if s:
      return '%s\n%s\n'%(COMMENT_TBL[tag],'\n'.join(s))

  def xml2crm_option(self,tag):
    #
    # property
    #   <option>=<value> [<option>=<value>...]
    # /
    # rsc_defaults
    #   <option>=<value> [<option>=<value>...]
    # /
    # op_defaults
    #   <option>=<value> [<option>=<value>...]
    #
    s = []
    for x in self.root.getElementsByTagName(tag):
      for y in x.childNodes:
        s.append('%s="%s"'%(y.getAttribute('name'),y.getAttribute('value')))
    if s:
      return '%s\n%s %s\n'%(COMMENT_TBL[tag],tag,' \\\n\t'.join(s))

  def xml2crm_resources(self,tags):
    #
    # group <name> <rsc> [<rsc>...]
    #   [meta   attr_list]
    #   [params attr_list]
    # /
    # clone <name> <rsc>
    #   [meta   attr_list]
    #   [params attr_list]
    # /
    # ms <name> <rsc>
    #   [meta   attr_list]
    #   [params attr_list]
    #
    s = []; prev = None
    for r in self.xml_get_childs(self.rr,tags):
      x = ''; y = []
      if prev != r.nodeName:
        x = '%s\n'%COMMENT_TBL[r.nodeName]
        prev = r.nodeName
      y.append('%s%s %s'%(x,r.nodeName,r.getAttribute('id')))
      for x in r.getElementsByTagName('rsc'):
        y.append(x.getAttribute('id'))
      self.xml2crm_attr(r,y)
      s.append(' \\\n\t'.join(y))
    if s:
      return '%s\n'%'\n\n'.join(s)

  def xml2crm_primitive(self):
    if not self.rr:
      return
    #
    # primitive <rsc> [<class>:[<provider>:]]<type>
    #   [params      attr_list]
    #   [meta        attr_list]
    #   [utilization attr_list]
    #   [operations  id_spec]
    #     [op op_type [<attribute>=<value>...]...]]
    #
    s = []; tag = 'primitive'
    for p in self.rr.getElementsByTagName(tag):
      y = []; z = []
      if p.getAttribute('class'):
        z.append(p.getAttribute('class'))
      if p.getAttribute('provider'):
        z.append(p.getAttribute('provider'))
      z.append(p.getAttribute('type'))
      y.append('primitive %s %s'%(p.getAttribute('id'),':'.join(z)))
      self.xml2crm_attr(p,y,PRIM_ATTR_TYPE[:-1])
      self.xml2crm_attr(p,y,PRIM_ATTR_TYPE[-1:])
      for o in [o for x in p.getElementsByTagName('op') for o in x.childNodes]:
        z = []
        for x in o.childNodes:  # <nv>
          z.append(' %s="%s"'%(x.getAttribute('name'),x.getAttribute('value')))
        if len(z):
          y.append('op %s%s'%(o.nodeName,''.join(z)))
      s.append(' \\\n\t'.join(y))
    if s:
      return '%s\n%s\n'%(COMMENT_TBL[tag],'\n\n'.join(s))

  def xml2crm_location(self):
    #
    # location <id> <rsc> {rules}
    #
    # rules ::
    #   rule [$role=<role>] <score>: <expression>
    #   [rule [$role=<role>] <score>: <expression>...]
    #
    s = []; tag = 'location'; seq = 0
    for i,l in enumerate(self.root.getElementsByTagName(tag)):
      r = l.getAttribute('rsc')
      s.append('location rsc_location-%s-%d %s'%(r,i+1,r))
      for x in l.getElementsByTagName('rule'):
        s.append(' \\\n\trule %s: '%x.getAttribute('score'))
        t = x.getAttribute('type')
        if t == 'uname':
          s.append('#uname eq %s'%x.getAttribute('node'))
        elif t in ['pingd','diskd']:
          a = x.getAttribute('attr')
          s.append('not_defined %s or %s '%(a,a))
          if t == 'pingd':
            s.append('lt %s'%x.getAttribute('value'))
          elif t == 'diskd':
            s.append('eq ERROR')
      seq = i+1
      s.append('\n')
    for i,l in enumerate(self.root.getElementsByTagName('locexpert')):
      r = l.getAttribute('rsc')
      s.append('location rsc_location-%s-%d %s'%(r,seq+i+1,r))
      for x in l.getElementsByTagName('rule'):
        s.append(' \\\n\trule')
        if x.getAttribute('id_spec'):
          s.append(' %s'%x.getAttribute('id_spec'))
        if x.getAttribute('role'):
          s.append(' $role="%s"'%x.getAttribute('role'))
        if x.getAttribute('score'):
          s.append(' %s: '%x.getAttribute('score'))
        z = []
        for y in x.getElementsByTagName('exp'):
          o = y.getAttribute('op')
          if o.lower() in UNARY_OP:
            z.append('%s %s'%(o,y.getAttribute('attribute')))
          else:
            z.append('%s %s %s'
              %(y.getAttribute('attribute'),o,y.getAttribute('value')))
        if z:
          s.append((' %s '%x.getAttribute('bool_op')).join(z))
      s.append('\n')
    if s:
      return '%s\n%s'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_colocation(self):
    #
    # colocation <id> <score>: <rsc>[:<role>] <with-rsc>[:<role>]
    #
    s = []; tag = 'colocation'
    for i,x in enumerate(self.root.getElementsByTagName(tag)):
      r = x.getAttribute('rsc')
      w = x.getAttribute('with-rsc')
      e = x.getAttribute('score')
      s.append('colocation rsc_colocation-%s-%s-%d %s: %s'%(r,w,i+1,e,r))
      if x.getAttribute('rsc-role'):
        s.append(':%s'%x.getAttribute('rsc-role'))
      s.append(' %s'%w)
      if x.getAttribute('with-rsc-role'):
        s.append(':%s'%x.getAttribute('with-rsc-role'))
      s.append('\n')
    if s:
      return '%s\n%s'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_order(self):
    #
    # order <id> <score>: <first-rsc>[:<action>] <then-rsc>[:<action>]
    #   [symmetrical=<bool>]
    #
    s = []; tag = 'order'
    for i,x in enumerate(self.root.getElementsByTagName(tag)):
      f = x.getAttribute('first-rsc')
      t = x.getAttribute('then-rsc')
      e = x.getAttribute('score')
      s.append('order rsc_order-%s-%s-%d %s: %s'%(f,t,i+1,e,f))
      if x.getAttribute('first-action'):
        s.append(':%s'%x.getAttribute('first-action'))
      s.append(' %s'%t)
      if x.getAttribute('then-action'):
        s.append(':%s'%x.getAttribute('then-action'))
      if x.getAttribute('symmetrical'):
        s.append(' symmetrical=%s'%x.getAttribute('symmetrical'))
      s.append('\n')
    if s:
      return '%s\n%s'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_ftopo(self):
    #
    # fencing_topology
    #   [<node>:] <rsc>[,<rsc>...] [<rsc>[,<rsc>...] ...]
    #   [[<node>:] <rsc>[,<rsc>...] [<rsc>[,<rsc>...] ...]]
    #
    tag = 'ftopo'; ftopos = []
    for x in self.root.getElementsByTagName(tag):
      ftopos.append(
        (x.getAttribute('node'),x.getAttribute('rsc'),x.getAttribute('index')))
    if len(ftopos) == 0:
      return
    ftopos.sort(key=lambda ftopo: ftopo[2])  #      sort by 'index' first,
    ftopos.sort(key=lambda ftopo: ftopo[0])  # then sort by 'node'
    s = []; z = []; node = ''; idx = ''
    s.append('fencing_topology')
    for x in ftopos:
      if node != x[0] or not idx:
        z.append(' \\\n\t')
      if node != x[0]:
        node = x[0]; idx = ''
        z.append('%s: '%node)
      if idx != x[2]:
        if idx:
          z.append(' ')
        idx = x[2]
      else:
        z.append(',')
      z.append('%s'%x[1])
    s.append(''.join(z))
    return '%s\n%s\n'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_ticket(self):
    #
    # rsc_ticket <id> <ticket_id>: <rsc>[:<role>]
    #   [loss-policy=<loss_policy_action>]
    #
    s = []; tag = 'ticket'
    for i,x in enumerate(self.root.getElementsByTagName(tag)):
      t = x.getAttribute('ticket')
      r = x.getAttribute('rsc')
      s.append('rsc_ticket rsc_ticket-%s-%d %s: %s'%(t,i+1,t,r))
      if x.getAttribute('role'):
        s.append(':%s'%x.getAttribute('role'))
      if x.getAttribute('loss-policy'):
        s.append(' loss-policy=%s'%x.getAttribute('loss-policy'))
      s.append('\n')
    if s:
      return '%s\n%s'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_config(self):
    s = []; tag = 'config'
    for x in self.root.getElementsByTagName(tag):
      s.append('%s\n'%x.getAttribute('data'))
    if s:
      return '%s\n%s'%(COMMENT_TBL[tag],''.join(s))

  def xml2crm_attr(self,node,s,nodes=None):
    #
    # params attr_list / meta attr_list
    # attr_list :: <attr>=<val> [<attr>=<val>...]
    # /
    # attributes attr_list / utilization attr_list
    # attr_list :: <param>=<value> [<param>=<value>...]
    #
    if not nodes:
      nodes = ATTRIBUTE_TYPE
    for x in self.xml_get_childs(node,nodes):
      for i,y in enumerate(x.childNodes):  # <nv>
        if i == 0:
          s.append(x.nodeName)
        s.append('\t%s="%s"'%(y.getAttribute('name'),y.getAttribute('value')))

  '''
    文字列をファイル・ディスクリプタに書き出す
    [引数]
      string : 出力対象
    [戻り値]
      True  : OK
      False : NG
  '''
  def write(self,string):
    if self.output in [sys.stdout,sys.stderr]:
      try:
        sys.stdout.write(string)
      except Exception,msg:
        log.error(u'crmコマンドの出力に失敗しました。[標準出力]')
        log.error(msg)
        return False
    else:
      try:
        fd = codecs.open(self.output,'w',CODE_OUTFILE)
      except Exception,msg:
        log.error(u'ファイルのオープンに失敗しました。[%s]'%self.output)
        log.error(msg)
        return False
      try:
        fd.write(string)
      except Exception,msg:
        log.error(u'crmファイルの出力に失敗しました。[%s]'%self.output)
        log.error(msg)
        os.unlink(self.output)
        return False
      try:
        fd.close()
      except Exception,msg:
        log.error(u'ファイルのクローズに失敗しました。[%s]'%self.output)
        log.error(msg)
        return False
    return True

  def debug_input(self,clmd,RIl,csvl):
    if log.level >= Log.DEBUG1:
      s = ['[%s:%s]'%(self.mode[0],self.mode[1])]
      for x in RIl:
        s.append('(%d)RI[%s]'%(x+1,csvl[x]))
      for k,x in dict2list(clmd):
        s.append('(%d)%s[%s]'%(x+1,k,csvl[x]))
      log.debug1_l(' '.join(s))

  def xml_debug(self):
    if log.level >= Log.DEBUG1:
      log.debug1(u'XML (CSVデータから生成) を出力します。')
      log.stderr(self.root.toprettyxml(indent='  '))


class Log:
  LOGLV_TBL = {
    'ERROR':  0,
    'WARN':   1,
    'notice': 2,
    'info':   3,
    'debug':  4,
    'debug1': 5
  }
  ERROR  = LOGLV_TBL['ERROR']
  WARN   = LOGLV_TBL['WARN']
  NOTICE = LOGLV_TBL['notice']
  INFO   = LOGLV_TBL['info']
  DEBUG  = LOGLV_TBL['debug']
  DEBUG1 = LOGLV_TBL['debug1']

  level_maxlen = 0
  for x in LOGLV_TBL:
    if len(x) > level_maxlen:
      level_maxlen = len(x)

  def __init__(self):
    self.level = self.ERROR
    self.printitem_file = None
    self.printitem_lineno = 0

  def error(self,msg):
    self.print2e(self.ERROR,msg)
  def error_f(self,msg):
    self.print2e(self.ERROR,msg,self.printitem_file)
  def error_l(self,msg):
    self.print2e(self.ERROR,msg,self.printitem_file,self.printitem_lineno)

  def warn(self,msg):
    self.print2e(self.WARN,msg)
  def warn_l(self,msg):
    self.print2e(self.WARN,msg,self.printitem_file,self.printitem_lineno)

  def notice(self,msg):
    self.print2e(self.NOTICE,msg)
  def notice_l(self,msg):
    self.print2e(self.NOTICE,msg,self.printitem_file,self.printitem_lineno)

  def info(self,msg):
    self.print2e(self.INFO,msg)
  def info_f(self,msg):
    self.print2e(self.INFO,msg,self.printitem_file)
  def info_l(self,msg):
    self.print2e(self.INFO,msg,self.printitem_file,self.printitem_lineno)

  def debug_f(self,msg):
    self.print2e(self.DEBUG,msg,self.printitem_file)
  def debug_l(self,msg):
    self.print2e(self.DEBUG,msg,self.printitem_file,self.printitem_lineno)

  def debug1(self,msg):
    self.print2e(self.DEBUG1,msg)
  def debug1_l(self,msg):
    self.print2e(self.DEBUG1,msg,self.printitem_file,self.printitem_lineno)

  def innererr(self,msg,info=None):
    self.error(u'内部エラーが発生しました。(%s)'%msg)
    if info:
      self.info_f(info)

  fmterrmsg = u'フォーマット・エラーが見つかりました。'
  def fmterr_f(self,info=None):
    self.error_f(self.fmterrmsg)
    if info:
      self.error_f(info)
  def fmterr_l(self,info=None):
    self.error_l(self.fmterrmsg)
    if info:
      self.error_l(info)

  def quitmsg(self,ret):
    if ret == 1:
      self.stderr(u'crmファイル生成中にエラーが発生しました。処理を中止します。\n')
    elif ret == 2 and self.WARN > self.level:
      self.stderr(u'警告が発生しています。'
                  u"警告のログを出力するには '-V' オプションを指定してください。\n")

  def print2e(self,level,msg,file=None,lineno=None):
    if level == self.ERROR:
      global errflg; errflg = True
      global errflg2; errflg2 = True
    elif level == self.WARN:
      global warnflg; warnflg = True
    if level > self.level:
      return
    for k in [k for (k,x) in self.LOGLV_TBL.items() if x == level]:
      level = k + ' ' * (self.level_maxlen - len(k))
    if file:
      if lineno:
        sys.stderr.write('%s: %s(L%s): %s\n'%(level,file,lineno,msg))
      else:
        sys.stderr.write('%s: %s: %s\n'%(level,file,msg))
    else:
      sys.stderr.write('%s: %s\n'%(level,msg))

  def stderr(self,msg):
    sys.stderr.write(msg)


'''
  リスト（要素）の文字コードをUnicodeに変換
    ・要素の前後の全半角空白/タブ/改行を削除(※)
    ・文字列中の改行を半角空白に置換(※)
  [引数]
    tl       : 変換対象のリスト
    encoding : `encoding' -> Unicode に変換
    do_fmt   : ※の処理を行うか
  [戻り値]
    True  : OK
    False : NG
'''
def unicode_listitem(tl,encoding,do_fmt):
  for i,data in [(i,x) for (i,x) in enumerate(tl) if x]:
    if do_fmt:
      while data.count('\n\n'):
        data = data.replace('\n\n','\n')
    try:
      if do_fmt:
        tl[i] = del_blank(unicode(data.replace('\n',' '),encoding))
      else:
        tl[i] = unicode(data,encoding)
    except:
      log.error(u'データのUnicodeへの変換に失敗しました。')
      return False
  return True

'''
  データの文字コードを判定
  [引数]
    data : 対象データ
  [戻り値]
    エンコーディング（を示す文字列）
'''
def detect_char(data):
  if not has_non_ascii(data.replace('\n','')):
    e = 'ascii'
    log.debug_f(u'CSVファイルの文字エンコーディング [%s]'%e)
    return e
  for e in ['euc_jp','euc_jis_2004','euc_jisx0213','iso2022_jp','iso2022_jp_1',
            'iso2022_jp_2','iso2022_jp_2004','iso2022_jp_3','iso2022_jp_ext']:
    try:
      unicode(data,e)
      log.debug_f(u'CSVファイルの文字エンコーディング [%s]'%e)
      break
    except:
      pass
  else:
    for e in ['utf-8','cp932','shift_jis','shift_jis_2004','shift_jisx0213']:
      try:
        unicode(data,e)
        log.debug_f(u'CSVファイルの文字エンコーディング [%s]'%e)
        return e
      except:
        pass
  log.error(u'CSVファイルの文字エンコーディングが未対応のエンコーディングです。')
  return None

'''
  文字列の前後空白（全半角空白/タブ文字）を取り除く
  [引数]
    string : 対象文字列
  [戻り値]
    処理後の文字列
'''
def del_blank(string):
  s = string.strip().strip(u'　')
  if string == s:
    return s
  return del_blank(s)

'''
  非ASCII文字チェック
  [引数]
    string : 対象文字列
  [戻り値]
    True  : 非ASCII文字あり
    False : 非ASCII文字なし
'''
regexp = re.compile(r'[^\x20-\x7E]')
def has_non_ascii(string):
  return regexp.search(string) is not None

'''
  辞書（dict）データをリストに変換
  [引数]
    d          : 辞書
    value_only : True  : リストの要素を (値)      にする
                 False : リストの要素を (キー,値) にする
  [戻り値]
    辞書データのリスト
'''
def dict2list(d,value_only=None):
  l = d.values()
  l.sort()
  if value_only:
    return l
  return [(k,x) for y in l for (k,x) in d.items() if x == y]

'''
  列番号をExcelでの列名に変換
  [引数]
    pos : 列番号（0～255）
  [戻り値]
    列名（'列A'～'列IV'）
'''
def pos2clm(pos):
  s = ''
  pos = int(pos) + 1
  while pos:
    s = chr((pos - 1) % 26 + 65) + s
    pos = (pos - 1) / 26
  return u'列' + s


try:
  sys.stdout = codecs.getwriter(CODE_PLATFORM)(sys.stdout)
  sys.stderr = codecs.getwriter(CODE_PLATFORM)(sys.stderr)
except Exception:
  sys.stderr.write('failed to encode stdout and stderr.\n')
  sys.exit(1)

if __name__ == '__main__':
  log = Log()
  ret = Crm().generate()
  log.quitmsg(ret)
  sys.exit(ret)
