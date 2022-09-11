"""

  """
##

import pandas as pd
from githubdata import GithubData
import json
from mirutil import utils as mu
from mirutil.df_utils import save_as_prq_wo_index as sprq

class RepoUrls:
  targ = 'https://github.com/imahdimir/d-tse_ir-industry-subIndustry'
  cur = 'https://github.com/imahdimir/raw-d-industry-subIndustry-Heidari_Data'

ru = RepoUrls()

def main():

  pass

  ##
  df = pd.read_excel('data.xlsx')

  ##
  for cn in df.columns:
    msk = df[cn].isna()
    df.loc[msk, cn] = None

  ##
  ren = {
      'name' : 'CompanyName',
      'Symbol' : 'Ticker',
      'Industry' : 'Industry',
      'Sub_industry' : 'SubIndustry',
      'ISN': 'ic',
      }
  ##
  df.rename(columns=ren, inplace=True)
  ##
  df = df[ren.values()]
  ##
  with open('META.json', 'r') as fi:
    meta = json.load(fi)
  ##
  cdt = meta['createDate']
  ##
  df['Date'] = cdt
  ##
  rp_targ = GithubData(ru.targ)
  rp_targ.clone()
  ##
  dftp = rp_targ.data_fp
  dft = pd.read_parquet(dftp)
  ##

  ##
  df1 = pd.concat([dft, df], ignore_index=True)
  ##
  df1 = df1.sort_values(by=['Date', 'CompanyName'], ascending = False)
  ##
  df1 = df1.drop_duplicates(subset=ren.values(), keep='first')
  ##
  sprq(df1, dftp)
  ##
  tokp = '/Users/mahdi/Dropbox/tok.txt'
  tok = mu.get_tok_if_accessible(tokp)
  ##
  msg = 'added Heidari data from: '
  msg += ru.cur
  ##
  rp_targ.commit_and_push(msg, user = rp_targ.user_name, token = tok)

  ##
  rp_targ.rmdir()


  ##

##
if __name__ == "__main__":
  main()

##