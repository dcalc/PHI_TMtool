import streamlit as st
from TMclass import *
import numpy as np
import datetime
# import pandas as pd

def observation(i,phi):
	i = str(i)
	mode = st.selectbox(
	     i+') Select Observation Mode',
	     ('FDT', 'HRT'))
	index = st.selectbox(
	     i+') Select partition',
	     (1,2))

	with st.form(i+') Time'):
		start_date = st.date_input(i+') Insert starting date',value=datetime.date(2022,1,1))

		start_time = st.time_input(i+')Insert starting time',datetime.time(0,0,0))

		start = datetime.datetime.combine(start_date, 
							start_time)


		end_date = st.date_input(i+')Insert ending date',start_date)

		end_time = st.time_input(i+')Insert ending time',datetime.time(0,0,0))

		end = datetime.datetime.combine(end_date, 
							end_time)
		
		cad = st.number_input(i+") Insert Cadence (in mins)",min_value=1.0, step=.1)

		st.form_submit_button()

	#start = start_date + start_time
	st.write('Duration:', end-start)

	# phi = PHI_MEMORY(start-datetime.timedelta(hours=1))

	#High cadence, 4 days, helio
	#t0 = datetime.datetime.fromisoformat('2022-02-01T00:00:00')
	#t1 = datetime.datetime.fromisoformat('2022-02-01T13:20:00')
	a0 = PHI_MODE(mode)

	with st.form(i+') Shape'):
		y = st.number_input(i+") Y axis",min_value=2048, max_value=2048,value=2048)
		x = st.number_input(i+") X axis",min_value=1, max_value=2048,value=2048)
		p = st.number_input(i+") P axis",min_value=1, max_value=4,value=4)
		wl = st.number_input(i+") L axis",min_value=1, max_value=6,value=6)
		st.form_submit_button()
	
	kw = a0.observation(start,end,cadence=cad,shape=(y,x,p,wl))
	lev = 'raw'
	phi.saving(index,**kw)
	t0 = a0.raw.end
	
	option = st.selectbox(
    i+') Do you want to process, crop or pack your dataset?',
    ('Processing', 'Cropping', 'Packing'))

	if option == 'Processing':
		with st.form(i+') Processing'):
			nout = st.number_input(i+") Insert number of processing outputs",min_value=3, max_value=5, value=5, step=1,\
							help='5 outputs: (Ic,VLoS,Bvec);\n3 outputs: (Ic,VLoS,BLoS)')
			t0d = st.date_input(i+') Insert starting processing date',value=t0.date(),min_value = t0.date())
			if t0d == t0.date():
				t0t_min = t0.time()
			else:
				t0t_min = datetime.time(0,0,0)
			t0t = st.time_input(i+') Insert starting processing time',t0t_min)
			t0 = datetime.datetime.combine(t0d, t0t)
			st.form_submit_button()

		kw = a0.processing(t0,ndata=-1,nout=nout,partialStore=0x00,level=lev)
		phi.saving(index,**kw)
		t0 = a0.proc.end
		st.write('Processing end:',t0)
		lev = 'proc'

		st.write(i+') The processed file will be compressed')

		with st.form(i+') Compressing'):
			nbits = st.selectbox(i+") Select number of bits",(2,3,4,5,6,16))
			t0d = st.date_input(i+') Insert starting compressing date',value=t0.date(),min_value = t0.date())
			if t0d == t0.date():
				t0t_min = t0.time()
			else:
				t0t_min = datetime.time(0,0,0)
			t0t = st.time_input(i+') Insert starting compressing time',t0t_min)
			t0 = datetime.datetime.combine(t0d, t0t)
			st.form_submit_button()
			
		kw = a0.compressing(t0, nbits = nbits, ndata = -1,level=lev)
		phi.saving(index,**kw)
		st.write('Compression end:',a0.compr.end)

	if option == 'Cropping':
		with st.form(i+') Cropping'):
			c = [1,1]
			c[0] = st.number_input(i+") Insert Crop along y axis",min_value=0,max_value=y)
			c[1] = st.number_input(i+") Insert Crop along x axis",min_value=0,max_value=x)
		
			t0d = st.date_input(i+') Insert starting cropping date',value=t0.date(),min_value = t0.date())
			if t0d == t0.date():
				t0t_min = t0.time()
			else:
				t0t_min = datetime.time(0,0,0)
			t0t = st.time_input(i+') Insert starting cropping time',t0t_min)
			t0 = datetime.datetime.combine(t0d, t0t)
			st.form_submit_button()

		kw = a0.cropping(t0,ndata=-1,crop=c,level=lev)
		phi.saving(index,**kw)
		t0 = a0.raw.crop.end
		lev = 'raw.crop'
		st.write('Cropping end:',t0)

		option_2 = st.selectbox(
		i+') Do you want to process or pack your dataset?',
		('Processing', 'Packing'))


		if option_2 == 'Processing':
			with st.form(i+') Processing'):
				nout = st.number_input(i+") Insert number of processing outputs",min_value=3, max_value=5, value=5, step=1,\
								help='5 outputs: (Ic,VLoS,Bvec);\n3 outputs: (Ic,VLoS,BLoS)')
				t0d = st.date_input(i+') Insert starting processing date',value=t0.date(),min_value = t0.date())
				if t0d == t0.date():
					t0t_min = t0.time()
				else:
					t0t_min = datetime.time(0,0,0)
				t0t = st.time_input(i+') Insert starting processing time',t0t_min)
				t0 = datetime.datetime.combine(t0d, t0t)
				st.form_submit_button()
			
			kw = a0.processing(t0,ndata=-1,nout=nout,partialStore=0x00,level=lev)
			phi.saving(index,**kw)
			t0 = a0.proc.crop.end
			lev = 'proc.crop'
			st.write('Processing end:',t0)

			st.write(i+') The cropped + processed data will be compressed')
			
			with st.form(i+') Compressing'):
				nbits = st.selectbox(i+") Select number of bits",(2,3,4,5,6,16),index=4)
				t0d = st.date_input(i+') Insert starting compressing date',value=t0.date(),min_value = t0.date())
				if t0d == t0.date():
					t0t_min = t0.time()
				else:
					t0t_min = datetime.time(0,0,0)
				t0t = st.time_input(i+') Insert starting compressing time',t0t_min)
				t0 = datetime.datetime.combine(t0d, t0t)
				st.form_submit_button()

			kw = a0.compressing(t0, nbits = nbits, ndata = -1,level=lev)
			phi.saving(index,**kw)
			st.write('Compression end:',a0.compr.crop.end)

		if option_2 == 'Packing':
			with st.form(i+') Packing'):
				t0d = st.date_input(i+') Insert starting packing date',value=t0.date(),min_value = t0.date())
				if t0d == t0.date():
					t0t_min = t0.time()
				else:
					t0t_min = datetime.time(0,0,0)
				t0t = st.time_input(i+') Insert starting packing time',t0t_min)
				t0 = datetime.datetime.combine(t0d, t0t)
				st.form_submit_button()

			kw = a0.packing(t0,ndata=-1,level=lev)
			phi.saving(index,**kw)
			t0 = a0.raw.pack.end
			lev = 'raw.pack'
			st.write('Packing end:',t0)

			st.write(i+') The cropped + packed data will be compressed')

			with st.form(i+') Compressing'):
				nbits = st.selectbox(i+") Select number of bits",(2,3,4,5,6,16),index=4)
				t0d = st.date_input(i+') Insert starting compressing date',value=t0.date(),min_value = t0.date())
				if t0d == t0.date():
					t0t_min = t0.time()
				else:
					t0t_min = datetime.time(0,0,0)
				t0t = st.time_input(i+') Insert starting compressing time',t0t_min)
				t0 = datetime.datetime.combine(t0d, t0t)
				st.form_submit_button()

			kw = a0.compressing(t0, nbits = nbits, ndata = -1,level=lev)
			phi.saving(index,**kw)
			st.write('Compression end:',a0.compr.pack.end)

	if option == 'Packing':
		with st.form(i+') Packing'):
			t0d = st.date_input(i+') Insert starting packing date',value=t0.date(),min_value = t0.date())
			if t0d == t0.date():
				t0t_min = t0.time()
			else:
				t0t_min = datetime.time(0,0,0)
			t0t = st.time_input(i+') Insert starting cropping time',t0t_min)
			t0 = datetime.datetime.combine(t0d, t0t)
			st.form_submit_button()

		kw = a0.packing(t0,ndata=-1,level=lev)
		phi.saving(index,**kw)
		t0 = a0.raw.pack.end
		lev = 'raw.pack'
		st.write('Packing end:',t0)

		st.write(i+') The cropped + packed data will be compressed')

		with st.form(i+') Compressing'):
			nbits = st.selectbox(i+") Select number of bits",(2,3,4,5,6,16),index=4)
			t0d = st.date_input(i+') Insert starting compressing date',value=t0.date(),min_value = t0.date())
			if t0d == t0.date():
				t0t_min = t0.time()
			else:
				t0t_min = datetime.time(0,0,0)
			t0t = st.time_input(i+') Insert starting compressing time',t0t_min)
			t0 = datetime.datetime.combine(t0d, t0t)
			st.form_submit_button()
			
		kw = a0.compressing(t0, nbits = nbits, ndata = -1,level=lev)
		phi.saving(index,**kw)
		st.write('Compression end:',a0.compr.pack.end)
	
	# st.write('Total amount of compressed data + metadata:',round(phi.part1.compr,1), 'MB')
	# st.write('number of datasets for this run:',a0.raw.n_datasets)

	printp(a0,gui=True)
	# df = pd.DataFrame.from_dict(phi.part1.history)

	return phi
####################################################################

st.set_page_config(page_title='TMtool',
				   page_icon=':satellite:',layout='wide')

st.title('SO/PHI Telemetry Tool v1.0')

start_date = st.sidebar.date_input('Insert starting reference date',value=datetime.date(2022,1,1))

start_date = datetime.datetime.combine(start_date, 
		                  datetime.time(0,0,0))

phi = PHI_MEMORY(start_date)

N = st.sidebar.number_input("How many observation do you need to run?",min_value=1, step=1)

c = st.beta_columns(N)

# clast = c[-1]

for i,ci in enumerate(c):
	with ci:
		phi = observation(i+1,phi)

# with clast:
#st.write("Plot!")
#st.line_chart(df)
# time_order = st.sidebar.checkbox('Plot in temporal order?')
# fig = phi.plot(1,time_ordered = time_order, bar=True)
# st.pyplot(fig)
ymax = st.sidebar.slider('y axis maximum',0,500,250,step=5)
st.pyplot(plot_tot(phi,ylim=(0,ymax),fig=True))