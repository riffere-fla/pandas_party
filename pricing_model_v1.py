import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
# from keras.models import Sequential
# from keras.layers import Dense, Dropout
# from keras.callbacks import EarlyStopping
# import matplotlib.pyplot as plt

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from tqdm import tqdm
import copy 
import warnings

import os
from torch import optim, nn, utils, Tensor
import lightning.pytorch as pl

# grab data (every game, pc, and day in sales cycle combination: 385570 x 13)
df = pd.read_csv("C:\\Users\\riffere\\Desktop\\pricing_round2.csv")
df = df[df['current_price'] > 0]

# make into a numpy arrays
dfx =df[['days_out', 'pc_num', 'original_price', 'rolling_30_seats_primary', 'min_listing_price','max_listing_price','mean_listing_price',
         'rolling_7_seats_logitix','rolling_14_atp_logitix','pct_change_logitix_atp', 'max_atp_sold', 'min_atp_sold', 'max_inventory', 'remaining_supply']]
X = np.array(dfx)
y = np.array(df[['current_price']])

# train test split and min max scaler
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1993, shuffle= True)
min_max_scaler = MinMaxScaler()
X_train = min_max_scaler.fit_transform(X_train)
X_test = min_max_scaler.transform(X_test)

# subset large df to test models quicker
total_rows = 385570
X_train = X_train[0 : total_rows]
X_test = X_test[0 : total_rows]
y_train = y_train[0 : total_rows]
y_test = y_test[0 : total_rows]

# Version 1
# https://github.com/christianversloot/machine-learning-articles/blob/main/how-to-create-a-neural-network-for-regression-with-pytorch.md

# batch_size: larger = faster, smaller = more epochs, mse is better
batch_size = 200

class MLP(nn.Module):
  '''
    Multilayer Perceptron for regression.
  '''
  def __init__(self):
    super().__init__()
    torch.manual_seed(1993)
    self.layers = nn.Sequential(nn.Linear(14, 36), 
                                    nn.ReLU(),
                                    nn.ReLU(),
                                    nn.Linear(36, 18), 
                                    nn.Dropout(p=0.1),
                                    nn.ReLU(),
                                    nn.ReLU(),
                                    nn.Dropout(p=0.1),
                                    nn.Linear(18, 6), 
                                    nn.ReLU(),
                                    nn.ReLU(),
                                    nn.Linear(6,1))


  def forward(self, x):
    '''
      Forward pass
    '''
    return self.layers(x)
  
class Dataset(torch.utils.data.Dataset):
  '''
  Prepare the dataset for regression
  '''

  def __init__(self, X, y, scale_data=True):
    if not torch.is_tensor(X) and not torch.is_tensor(y):
      # Apply scaling if necessary
      if scale_data:
          X = MinMaxScaler().fit_transform(X)
      self.X = torch.tensor(X, dtype=torch.float32)
      self.y = torch.tensor(y, dtype=torch.float32)

  def __len__(self):
      return len(self.X)

  def __getitem__(self, i):
      return self.X[i], self.y[i]
  
dataset = Dataset(X, y)
train_loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True, num_workers = 0)

mlp = MLP()
  
# Define the loss function and optimizer
loss_function = nn.MSELoss ()
optimizer = torch.optim.Adam(mlp.parameters(), lr=1e-4)

 # Run the training loop
for epoch in range(0, 5): # 5 epochs at maximum
    
    # Print epoch
    print(f'Starting epoch {epoch+1}')
    
    # Set current loss value
    current_loss = 0.0
    
    # Iterate over the DataLoader for training data
    for i, data in enumerate(train_loader, 0):
      
      # Get and prepare inputs
      inputs, targets = data
      inputs, targets = inputs.float(), targets.float()
      targets = targets.reshape((targets.shape[0], 1))
      
      # Zero the gradients
      optimizer.zero_grad()
      
      # Perform forward pass
      outputs = mlp(inputs)
      
      # Compute loss
      loss = loss_function(outputs, targets)
      
      # Perform backward pass
      loss.backward()
      
      # Perform optimization
      optimizer.step()
      
      # Print statistics
      current_loss += loss.item()
      if i % 10 == 0:
          #print('Loss after mini-batch %5d: %.3f' %
          #      (i + 1, current_loss / 500))
          current_loss = 0.0

  # Process is complete.
print('Training process has finished.')

pred = mlp.forward(torch.tensor(X_test, dtype = torch.float32))
numpred = pred.detach().numpy()

df1 = pd.DataFrame(numpred, columns = ['predicted'])
df2 = pd.DataFrame(y_test, columns = ['actual'])
df3 = pd.DataFrame(
    min_max_scaler.inverse_transform(X_test), columns = ["days_out", "pc_num", "original_price",
    "rolling_30_seats_primary", "min_listing_price", "max_listing_price", "mean_listing_price", "rolling_7_seats_logitix",
    "rolling_14_atp_logitix", "pct_change_logitix_atp", "max_atp_sold", "min_atp_sold", "max_inventory", "remaining_supply"])

final_df = pd.concat([df1,df2,df3], axis = 1)
#final_df.to_csv('C:\\Users\\riffere\\Desktop\\attempt2_model.csv')

#df_event_date = df[df['event_date'] == '2022-10-19']
temp = df.groupby(by = ["event_date","pc_num"]).min()['days_out']
temp = temp.to_frame()
temp.reset_index(inplace = True)
temp1 = temp.merge(right = df, how = 'left', on = ['pc_num', 'days_out', "event_date"])

dfx =temp1[['days_out', 'pc_num', 'original_price', 'rolling_30_seats_primary', 'min_listing_price','max_listing_price','mean_listing_price',
         'rolling_7_seats_logitix','rolling_14_atp_logitix','pct_change_logitix_atp', 'max_atp_sold', 'min_atp_sold', 'max_inventory', 'remaining_supply']]
X = np.array(dfx)
y = np.array(temp1[['current_price']])

min_max_scaler = MinMaxScaler()
X = min_max_scaler.fit_transform(X)
pred = mlp.forward(torch.tensor(X, dtype = torch.float32))
numpred = pred.detach().numpy()

df1 = pd.DataFrame(numpred, columns = ['predicted'])
df2 = pd.DataFrame(y, columns = ['actual'])
df3 = pd.DataFrame(
    min_max_scaler.inverse_transform(X), columns = ["days_out", "pc_num", "original_price",
    "rolling_30_seats_primary", "min_listing_price", "max_listing_price", "mean_listing_price", "rolling_7_seats_logitix",
    "rolling_14_atp_logitix", "pct_change_logitix_atp", "max_atp_sold", "min_atp_sold", "max_inventory", "remaining_supply"])

final_df = pd.concat([df1,df2,temp1], axis = 1)

new_df = final_df[['event_date', "pc_one", "predicted"]]
new_df = new_df.pivot(index = "event_date", columns = 'pc_one', values = "predicted")

new_df.to_csv('C:\\Users\\riffere\\Desktop\\new_df.csv')