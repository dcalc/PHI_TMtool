import streamlit as st
from TMclass import *
import numpy as np
import datetime
import pandas as pd

def observation(i,phi):
	i = str(i)
	mode = st.selectbox(
	     i+') Select Observation Mode',
	     ('FDT', 'HRT', 'CAL', 'FLUSH'))

	cad = st.number_input(i+") Insert Cadence (in mins)",min_value=1.0, step=.1)

	start_date = st.date_input(i+') Insert starting date')

	start_time = st.time_input(i+')Insert starting time',datetime.time(0,0,0))

	start = datetime.datetime.combine(start_date, 
		                  start_time)


	end_date = st.date_input(i+')Insert ending date',start_date)

	end_time = st.time_input(i+')Insert ending time',datetime.time(0,0,0))

	end = datetime.datetime.combine(end_date, 
		                  end_time)


	#start = start_date + start_time
	st.write('Duration:', end-start)

	# phi = PHI_MEMORY(start-datetime.timedelta(hours=1))

	#High cadence, 4 days, helio
	#t0 = datetime.datetime.fromisoformat('2022-02-01T00:00:00')
	#t1 = datetime.datetime.fromisoformat('2022-02-01T13:20:00')
	a0 = PHI_MODE(mode)

	
	y = st.number_input(i+") Y axis",min_value=1, max_value=2048)
	x = st.number_input(i+") X axis",min_value=2048, max_value=2048)
	p = st.number_input(i+") P axis",min_value=1, max_value=4)
	wl = st.number_input(i+") L axis",min_value=1, max_value=6)
	
	kw = a0.observation(start,end,cadence=cad,shape=(y,x,p,wl))
	phi.saving(1,**kw)
	t0 = a0.raw.end
	
	crop = st.checkbox(i+') Crop?')
	pack = st.checkbox(i+')Pack?')
	if crop:
		c = [1,1]
		c[0] = st.number_input(i+") Insert Crop along y axis",min_value=1.0)
		c[1] = st.number_input(i+") Insert Crop along x axis",min_value=1.0)
		kw = a0.cropping(t0,ndata=-1,crop=c,level='raw')
		phi.saving(1,**kw)
		t0 = a0.raw.crop.end
		if pack:
			kw = a0.packing(t0,ndata=-1,level='raw.crop')
			phi.saving(1,**kw)
			t0 = a0.raw.pack.end
			lev = 'raw.pack'
		else:
			kw = a0.processing(t0,ndata=-1,nout=5,partialStore=0x00,level='raw.crop')
			phi.saving(1,**kw)
			t0 = a0.proc.crop.end
			lev = 'proc.crop'
	else:
		if pack:
			kw = a0.packing(t0,ndata=-1,level='raw')
			phi.saving(1,**kw)
			t0 = a0.raw.pack.end
			lev = 'raw.pack'
		else:
			kw = a0.processing(t0,ndata=-1,nout=5,partialStore=0x00,level='raw')
			phi.saving(1,**kw)
			t0 = a0.proc.end
			lev = 'proc'
	
	
	kw = a0.compressing(t0, nbits = 6, ndata = -1,level=lev)
	phi.saving(1,**kw)

	st.write('amount of compressed data + metadata:',round(phi.part1.compr,1), 'MB')
	st.write('number of datasets:',a0.raw.n_datasets)

	#phi.plot(1,bar=True)
	
	df = pd.DataFrame.from_dict(phi.part1.history)

	return phi
####################################################################

st.set_page_config(layout='wide')

st.title('SO/PHI Telemetry Tool v0.0')

start_date = st.date_input('Insert starting reference date')
phi = PHI_MEMORY(start_date)

N = st.number_input("How many observation do you need to run?",min_value=1, step=1)

c = st.beta_columns(N)

# clast = c[-1]

for i,ci in enumerate(c):
	with ci:
		phi = observation(i+1,phi)

# with clast:
#st.write("Plot!")
fig = phi.plot(1,bar=True)
#st.line_chart(df)
st.pyplot(fig)