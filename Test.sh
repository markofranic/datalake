import pandas as pd
test=['test1','test2','test3']
df = pd.DataFrame(test)
df.to_csv('test.csv')