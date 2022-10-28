import datetime
import numpy as np
import matplotlib.pyplot as plt
import pickle
import os
import pandas as pd
import base64
import copy

__all__ = ['PHI_MEMORY', 'PARTITION', 'RAW','PROC','CROP','PACK','COMPR','BIN','EXTR','PHI_MODE',\
            'roundup','plot_tot','printp','final_plot', 'levels_did_out']


def roundup(x,base=8):
    return round(x) if x % base == 0 else round(x + base - x % base)

def _no_class(s,attr):
    t = type(getattr(s,attr))
    if t in [RAW,PROC,CROP,PACK,COMPR,EXTR,PHI_MODE,PARTITION,PHI_MEMORY]:
        return False
    else:
        return True

class PARTITION:
    # __all__ = ['total','free','occu','raw','proc','compr','crop','pack','cal','flush','history']
    
    pass
class RAW:
    # __all__ = ['start','end','cadence','X','Y','P','L','n_pix','n_bits','n_datasets','data','metadata','data_tot']
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Cadence: {self.cadence} mins')
        print(f'Shape: ({self.Y},{self.X},{self.P},{self.L})')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of raw datasets: {self.n_datasets}')
        print(f'Raw data+metadata size: {self.data} MB')
        print(f'Raw metadata size: {self.metadata} MB')
        
    pass
class PROC:

    def __init__(self,temp,level,did=None):
        
        # temp is the sublevel
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        self.level = level+'.proc'
        if did is None:
            start_did = int(temp.did)
            self.did  = str(start_did + 7000).rjust(10,'0')
        else:
            self.did = did
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.cpu_time = datetime.timedelta(minutes=35) # update 20211015 info by JH and Nestor
        self.interm_data_tot = 0
        self.time = datetime.timedelta(seconds=0)
        self.start = None
        self.end = None
        self.metadata = 0
        self.data = 0
        self.data_tot = 0

    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Processing time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Intermediate steps: {self.interm_steps}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Bit depth for intermediate data: {self.interm_n_bits}')
        print(f'Number of processed datasets: {self.n_datasets}')
        print(f'Processed data+metadata size: {self.data_tot} MB')
        
    pass
class COMPR:

    def __init__(self,temp,level):
        
        # temp is the sublevel
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        self.level = level+'.compr'
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.cpu_time = datetime.timedelta(minutes=35) # update 20211015 info by JH and Nestor
        self.interm_data_tot = 0
        self.data_tot = 0
        self.n_datasets = 0
        self.time = datetime.timedelta(seconds=0)
        self.n_outputs = temp.n_outputs
        self.start = None
        self.end = None
        self.time = datetime.timedelta(0)
        self.cpu_time = 0
        self.metadata = 0
        self.data = 0
        self.data_tot = 0

    def _compute_cpu_time(self):
        # self.cpu_time = datetime.timedelta(minutes=10) * self.X * self.Y * self.n_outputs / 100663296 #TBD
        self.cpu_time = datetime.timedelta(seconds=(self.data+self.metadata)*8) # 1 Mbit/s
        
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Compressing time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of compressed datasets: {self.n_datasets}')
        print(f'Compressed data+metadata size: {self.data_tot} MB')
        
    pass


class CROP:

    def __init__(self,temp,level,did=None):
        
        # temp is the sublevel
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        if did is None:
            start_did = int(temp.did)
            self.did  = str(start_did + 500).rjust(10,'0')
        else:
            self.did = did
        
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.interm_data_tot = 0
        self.data_tot = 0
        self.n_datasets = 0
        self.time = datetime.timedelta(seconds=0)
        self.n_outputs = temp.n_outputs
        self.level = level+'.crop'
        self.start = None
        self.end = None
        self.time = datetime.timedelta(0)
        self.cpu_time = 0
        self.metadata = 0
        self.data = 0
        self.data_tot = 0
        
    def _compute_cpu_time(self,temp):
        # self.cpu_time = datetime.timedelta(seconds=temp.n_bits * temp.X * temp.Y * self.n_outputs / 8 / 2**20 * 0.117 + 22.054) # Operations / ProcessingDuration
        if self.Y > 1024:
            self.cpu_time = datetime.timedelta(seconds=70)
        else:
            self.cpu_time = datetime.timedelta(seconds=35)

    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Cropping time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of cropped datasets: {self.n_datasets}')
        print(f'Cropped data+metadata size: {self.data_tot} MB')
        
    pass

class BIN:

    def __init__(self,temp,level,did=None):
        
        # temp is the sublevel
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        if did is None:
            start_did = int(temp.did)
            self.did  = str(start_did + 400).rjust(10,'0')
        else:
            self.did = did
        
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.interm_data_tot = 0
        self.data_tot = 0
        self.n_datasets = 0
        self.time = datetime.timedelta(seconds=0)
        self.n_outputs = temp.n_outputs
        self.level = level+'.bin'
        self.start = None
        self.end = None
        self.cpu_time = datetime.timedelta(seconds=60)
        self.metadata = 0
        self.data = 0
        self.data_tot = 0
        self.bin_factor = 1
        
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Cropping time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of cropped datasets: {self.n_datasets}')
        print(f'Cropped data+metadata size: {self.data_tot} MB')
        
    pass
class PACK:

    def __init__(self,temp,level,did=None):
        
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        if did is None:
            start_did = int(temp.did)
            self.did  = str(start_did + 40000000).rjust(10,'0')
        else:
            self.did = did
        
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.interm_data_tot = 0
        self.data_tot = 0
        self.n_datasets = 0
        self.time = datetime.timedelta(seconds=0)
        self.n_outputs = temp.n_outputs
        self.level = level+'.pack'
        self.start = None
        self.end = None
        self.cpu_time = 0
        self.metadata = 0
        self.data = 0
        self.data_tot = 0

    def _compute_cpu_time(self,temp):
        # self.cpu_time = datetime.timedelta(seconds = temp.n_bits * self.X * self.Y * self.n_outputs / 8 / 2**20 * 0.1471 + 27.32) # Operations / ProcessingDuration
        if self.Y > 1024:
            self.cpu_time = datetime.timedelta(seconds=90)
        else:
            self.cpu_time = datetime.timedelta(seconds=45)
    
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Packing time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of packed datasets: {self.n_datasets}')
        print(f'Packed data+metadata size: {self.data_tot} MB')
        
    pass

class EXTR:

    def __init__(self,temp,level,did=None):
        
        for k, v in temp.__dict__.items():
            if _no_class(temp,k):
                self.__dict__[k] = copy.deepcopy(v)
        
        if did is None:
            start_did = int(temp.did)
            self.did  = str(start_did + 200).rjust(10,'0')
        else:
            self.did = did
        
        self.n_datasets = 0
        self.not_datasets = temp.n_datasets
        self.interm_data_tot = 0
        self.data_tot = 0
        self.time = datetime.timedelta(seconds=0)
        self.n_outputs = 1
        self.level = level+'.extr'
        self.start = None
        self.end = None
        self.cpu_time = datetime.timedelta(seconds=120)
        self.metadata = 0
        self.data = 0
        

    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Packing time per data: {self.cpu_time} mins')
        print(f'X crop factor: {self.crop_x}')
        print(f'Y crop factor: {self.crop_y}')
        print(f'Number of outputs: {self.n_outputs}')
        print(f'Bit depth: {self.n_bits}')
        print(f'Number of packed datasets: {self.n_datasets}')
        print(f'Packed data+metadata size: {self.data_tot} MB')
        
    pass

def levels_did_out(phi_mode):
    def find_levels(s):
        levels = []
        t = []
        
        for k, v in s.__dict__.items():
            if k in ['raw','crop','bin','pack','extr','proc','compr']:
                t += [getattr(s,k)]
                levels += [t[-1].level]
        return levels, t

    levels = []
    temp = [[]]
    temp[0] += [phi_mode]
    i = 0
    while temp[i] != []:
        i += 1
        temp += [[]]
        for t in temp[i-1]:
            l,tt = find_levels(t); levels += l; temp[i] += tt;
        
    # temp = temp[:-1]
    temp = [t for tt in temp[1:] for t in tt]
    return levels, [t.did for t in temp], [t.n_datasets for t in temp]
class PHI_MODE():
    def __init__(self,mode):
        self.mode = mode
        if not mode in ['HRT','FDT','CAL','FLUSH']:
            print('Pleas insert a valid mode: HRT, FDT, CALIB or FLUSH')
            del self.mode
    
    def __checkMode__(self,req):
        if self.mode in req:
            pass
        else:
            raise ValueError(f'This function is not implemented for mode {self.mode}\nYou should use one of these modes: {req}')
    
    def _set_n_datasets(self,s,temp,ndata,procedure,verbose=True):
        diff = temp.n_datasets - (s.n_datasets + s.not_datasets)
        if diff > 0:
            # check if new operations have increased the number of datasets in the parent level
            s.not_datasets += diff
        if ndata == -1:
            s.this_run = s.not_datasets
            s.n_datasets = temp.n_datasets
            s.not_datasets = 0
        elif ndata <= s.not_datasets:
            s.this_run = ndata
            s.n_datasets += ndata
            s.not_datasets -= ndata
        elif ndata > s.not_datasets:
            if procedure != 'Extraction' and verbose:
                print(s.start,f'WARNING: '+procedure+', Exceeding the number of datasets, max should be '+str(s.not_datasets))
            # s.n_datasets = temp.n_datasets
            # s.this_run = temp.n_datasets
            # s.not_datasets = temp.n_datasets - ndata
            # s.this_run = ndata
            # s.n_datasets = ndata
            # s.not_datasets = temp.n_datasets - ndata
            s.n_datasets += ndata
            s.not_datasets -= ndata
            s.this_run = ndata

    def level_out(self,level):
        temp = self
        for l in level.split("."):
            temp = getattr(temp,l)
        return temp
        
    #############################################################################
    #############################################################################
    #############################################################################
        
    def observation(self,start,end,cadence,shape=(2048,2048,4,6),did='0000000000'):
#         class RAW:
#             pass
        self.__checkMode__(['HRT','FDT'])
        if hasattr(self,'raw'):
            self.raw.cadence = cadence
            self.raw.start = start
            if type(end) is int:
                self.raw.n_datasets += end
                self.raw.end = datetime.timedelta(minutes=end * self.raw.cadence) + self.raw.start
                self.raw.this_run = end
            else:
                self.raw.end = end
                self.raw.n_datasets += int((self.raw.end - self.raw.start).total_seconds() / (60*self.raw.cadence))
                self.raw.this_run = int((self.raw.end - self.raw.start).total_seconds() / (60*self.raw.cadence))
            self.raw.n_bits = 32
            self.raw.X = shape[1]; self.raw.Y = shape[0]; self.raw.P = shape[2]; self.raw.L = shape[3]
            self.raw.n_pix = self.raw.X*self.raw.Y*self.raw.P*self.raw.L
            self.raw.n_outputs = self.raw.L * self.raw.P
            
            # MB of raw metadata
            self.raw.metadata = 8 * self.raw.this_run
            # MB of raw data + metadata
            self.raw.data = roundup(self.raw.n_pix * self.raw.n_bits / 8e6) * self.raw.this_run #+ self.raw.metadata
            self.raw.data_tot += self.raw.data + self.raw.metadata

        else:
            self.raw = RAW()
            self.raw.level = 'raw'
            self.raw.did = did
            self.raw.cadence = cadence
            self.raw.start = start

            if type(end) is int:
                self.raw.n_datasets = end
                self.raw.end = datetime.timedelta(minutes=end * self.raw.cadence) + self.raw.start
            else:
                self.raw.end = end
                self.raw.n_datasets = int((self.raw.end - self.raw.start).total_seconds() / (60*self.raw.cadence))
        
            self.raw.n_bits = 32
            self.raw.X = shape[1]; self.raw.Y = shape[0]; self.raw.P = shape[2]; self.raw.L = shape[3]
            self.raw.n_pix = self.raw.X*self.raw.Y*self.raw.P*self.raw.L
            self.raw.n_outputs = self.raw.L * self.raw.P
            self.raw.this_run = self.raw.n_datasets
            # MB of raw metadata
            self.raw.metadata = 8 * self.raw.n_datasets
            # MB of raw data + metadata
            self.raw.data = roundup(self.raw.n_pix * self.raw.n_bits / 8e6) * self.raw.n_datasets #+ self.raw.metadata
            self.raw.data_tot = self.raw.data + self.raw.metadata
        
#         self.raw.memory_flag = False
        
        return {'tm_type':type(self.raw), 'val':self.raw.data + self.raw.metadata,\
                'key':'raw', 'start':self.raw.start, 'end':self.raw.end}
    
    #############################################################################
    #############################################################################
    #############################################################################
    
    def processing(self,start,ndata=-1,partialStore=0x00,nout=5,level='raw',did = None, verbose=True):
        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)

        try:
            s = self.level_out(level+'.proc')
            self._set_n_datasets(s,temp,ndata,'Processing',verbose)
            s.start = start
            s.nout = nout
            if did is not None:
                if int(did) < int(s.did):
                    s.did = did
        except:
            # print(level+'.proc not existing yet: initializing')
            try:
                temp.proc = PROC(temp,level,did)
                s = temp.proc
                self._set_n_datasets(s,temp,ndata,'Processing',verbose)
                s.n_outputs = nout
                s.start = start

            except:
                print(sublevel,'sublevel do not exist: cannot proceed further with requested level',".".join(level))
     
        s.end = s.start + s.cpu_time * s.this_run
        s.time += s.end - s.start
        # intermediate processing steps
        if partialStore == 0x00:
            s.interm_steps = 0 # updated with S/W update in September 2022
        if partialStore == 0xFF:
            s.interm_steps = 5
        s.interm_n_bits = 32
        s.n_bits = 16
         
        #MB of intermediate data + metadata
        s.interm_metadata = 8 * s.interm_steps * s.this_run
        s.interm_data = roundup(round(temp.n_outputs*s.X*s.Y,0)*\
                                 s.interm_n_bits / 8e6) * s.interm_steps * s.this_run
        s.interm_data_tot += s.interm_data + s.interm_metadata

        #MB of processed data + metadata
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.X*s.Y,0)*\
                          s.n_bits / 8e6 * s.n_outputs) * s.this_run
        s.data_tot += s.data + s.metadata

        return {'tm_type':PROC, 'val':s.data + s.metadata + s.interm_data + s.interm_metadata,\
                'key':'proc', 'start':s.start, 'end':s.end}

     
    #############################################################################
    #############################################################################
    #############################################################################
        
    def cropping(self,start,crop,ndata=-1,level='raw',did=None,verbose=True):
        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)

        try:
            s = self.level_out(level+'.crop')
            self._set_n_datasets(s,temp,ndata,'Cropping',verbose)
            s.start = start
            if did is not None:
                if int(did) < int(s.did):
                    s.did = did
        except:
            # print(level+'.crop not existing yet: initializing')
            try:
                temp.crop = CROP(temp,level,did)
                s = temp.crop
                self._set_n_datasets(s,temp,ndata,'Cropping',verbose)
                
                s.start = start

            except Exception as exc:
                print(exc)
                print(sublevel,'sublevel do not exist: cannot proceed further with requested level',".".join(level))
               
        if isinstance(crop,int):
            crop_x = crop
            crop_y = crop
        elif isinstance(crop,list):
            if len(crop) == 2:
                crop_x = crop[1]
                crop_y = crop[0]
        else:
            raise ValueError('crop variable must be an integer or a list with two integer elements (now only power of 2 are accepted)')

        s.X = crop_x
        s.Y = crop_y

        # s.cpu_time = datetime.timedelta(seconds=120) * self.raw.X * self.raw.Y * s.n_outputs / 100663296 #TBD according to FCP_710
        s._compute_cpu_time(temp)
        
        s.end = s.start + s.cpu_time * s.this_run
        s.time += s.end - s.start
        
        # s.end = s.start + s.cpu_time * s.this_run
        
        s.n_bits = temp.n_bits
        if 'pack' in level:
            s.n_bits = 16
        
        #MB of processed data + metadata
        
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.X*s.Y,0)*\
                                s.n_bits * s.n_outputs / 8e6)* s.this_run
        
        s.data_tot += s.data + s.metadata
        return {'tm_type':CROP, 'val':s.data + s.metadata,\
                'key':'crop', 'start':s.start, 'end':s.end}
    

    #############################################################################
    #############################################################################
    #############################################################################
        
    def binning(self,start,bin_factor,ndata=-1,level='raw',did=None,verbose=True):
        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)

        try:
            s = self.level_out(level+'.bin')
            self._set_n_datasets(s,temp,ndata,'Binning',verbose)
            s.start = start
            if did is not None:
                if int(did) < int(s.did):
                    s.did = did
        except:
            # print(level+'.bin not existing yet: initializing')
            try:
                temp.bin = BIN(temp,level,did)
                s = temp.bin
                self._set_n_datasets(s,temp,ndata,'Binning',verbose)
                
                s.start = start

            except Exception as exc:
                print(exc)
                print(sublevel,'sublevel do not exist: cannot proceed further with requested level',".".join(level))
        
        s.bin_factor = bin_factor
        s.X = int(temp.X/bin_factor)
        s.Y = int(temp.Y/bin_factor)

        s.end = s.start + s.cpu_time * s.this_run
        s.time += s.end - s.start
        
        # s.end = s.start + s.cpu_time * s.this_run
        
        s.n_bits = temp.n_bits
        if 'pack' in level:
            s.n_bits = 16
        
        #MB of processed data + metadata
        
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.X*s.Y,0)*\
                                s.n_bits * s.n_outputs / 8e6)* s.this_run
        
        s.data_tot += s.data + s.metadata
        return {'tm_type':BIN, 'val':s.data + s.metadata,\
                'key':'crop', 'start':s.start, 'end':s.end}
    
     
    #############################################################################
    #############################################################################
    #############################################################################
    
    def compressing(self,start,nbits,ndata,level = 'proc',verbose=True):
        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)

        try:
            s = self.level_out(level+'.compr')
            self._set_n_datasets(s,temp,ndata,'Compression',verbose)
            s.start = start
            
        except:
            # print(level+'.compr not existing yet: initializing')
            try:
                temp.compr = COMPR(temp,level)
                s = temp.compr
                self._set_n_datasets(s,temp,ndata,'Compression',verbose)
                
                s.start = start

            except:
                print(level,'sublevel do not exist: cannot proceed further with requested level',".".join(level))

        s.n_bits = nbits
            
        #MB of compressed data + metadata
        if 'proc' in level:
            s.metadata = 90e-3*s.n_outputs*s.this_run
        else:
            s.metadata = 9e-3*s.this_run

        s.data = (round(s.X*s.Y,0)*\
                                s.n_bits / 8e6 * s.n_outputs) * s.this_run #processed metadata 90*n_outputs kB, 0.7 MB before
        
        s._compute_cpu_time()
        
        s.end = s.start + s.cpu_time# + s.cpu_time * s.this_run
        s.time += s.end - s.start
        s.data_tot += s.data + s.metadata

        return {'tm_type':COMPR, 'val':s.data+s.metadata,\
                'key':'compr', 'start':s.start, 'end':s.end}
    
    
    #############################################################################
    #############################################################################
    #############################################################################
     
        
    def packing(self,start,ndata=-1,level='raw',did=None,verbose=True):

        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)

        try:
            s = self.level_out(level+'.pack')
            self._set_n_datasets(s,temp,ndata,'Packing',verbose)
            s.start = start
            if did is not None:
                if int(did) < int(s.did):
                    s.did = did
        except:
            # print(level+'.pack not existing yet: initializing')
            try:
                temp.pack = PACK(temp,level,did)
                s = temp.pack
                self._set_n_datasets(s,temp,ndata,'Packing',verbose)
                
                s.start = start

            except Exception as exc:
                print(exc)
                print(level,'sublevel do not exist: cannot proceed further with requested level',level+".pack")

        # s.cpu_time = datetime.timedelta(seconds=120) * self.raw.X * self.raw.Y * s.n_outputs / 100663296 #TBD according to FCP_710
        s._compute_cpu_time(temp)
        
        s.end = s.start + s.cpu_time * s.this_run
        s.time += s.end - s.start
        
        # s.end = s.start + s.cpu_time * s.this_run
        
        s.n_bits = 16
        
        #MB of processed data + metadata
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.X*s.Y,0)*\
                                s.n_bits * s.n_outputs / 8e6) * s.this_run
        
        s.data_tot += s.data + s.metadata
        return {'tm_type':PACK, 'val':s.data + s.metadata,\
                'key':'pack', 'start':s.start, 'end':s.end}

    #############################################################################
    #############################################################################
    #############################################################################
    
    def extract(self,start,level = 'proc',did=None):

        self.__checkMode__(['HRT','FDT'])
        sublevel = '.'.join(level.split('.')[:-1])
        temp = self.level_out(level)
        ndata = 1
        try:
            s = self.level_out(level+'.extr')
            self._set_n_datasets(s,temp,ndata,'Extraction')
            s.start = start
            if did is not None:
                s.did = did
        except:
            # print(level+'.extr not existing yet: initializing')
            try:
                temp.extr = EXTR(temp,level,did)
                s = temp.extr
                self._set_n_datasets(s,temp,ndata,'Extraction')
                
                s.start = start

            except:
                print(level,'sublevel do not exist: cannot proceed further with requested level',".".join(level))

        #MB of compressed data + metadata
        
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.X*s.Y,0)*s.n_bits / 8e6 * s.n_outputs) * s.this_run
    
        s.end = s.start + s.cpu_time * s.this_run
        s.time += s.end - s.start
        s.data_tot += s.data + s.metadata

        return {'tm_type':EXTR, 'val':s.data + s.metadata,\
                'key':level.split('.')[-1], 'start':s.start, 'end':s.end}
    
    
    
    #############################################################################
    #############################################################################
    ############################################################################# 
        

class PHI_MEMORY:
    import matplotlib.pyplot as plt
    import numpy as np
    
    def __init__(self,start,gui=False):
        
        if gui:
            self._load(start,gui)
        else:
            if type(start) == list or type(start) == str:
                self._load(start,gui)
            else:
                self.part1 = PARTITION()
                self.part2 = PARTITION()
                
                self.part1.total = 256e3 #MB
                self.part2.total = 256e3 #MB
            
                self.part1.free = self.part1.total
                self.part2.free = self.part2.total
                
                self.part1.occu = self.part1.total - self.part1.free
                self.part2.occu = self.part2.total - self.part2.free
                
                self.part1.flush = 0
                self.part2.flush = 0
                
                self.part1.raw = 0
                self.part2.raw = 0
                
                self.part1.proc = 0
                self.part2.proc = 0
                
                self.part1.compr = 0
                self.part2.compr = 0
                
                self.part1.cal = 0
                self.part2.cal = 0
                
                self.part1.crop = 0
                self.part2.crop = 0

                self.part1.pack = 0
                self.part2.pack = 0

                self.part1.history = {'occu':[0], 'raw':[0],\
                                    'proc':[0], 'compr':[0],'crop':[0],\
                                    'cal':[0], 'flush':[0], 'pack':[0],\
                                    'start':[start],'end':[start],'type':[type(self)]}
                self.part2.history = {'occu':[0], 'raw':[0],\
                                    'proc':[0], 'compr':[0],'crop':[0],\
                                    'cal':[0], 'flush':[0], 'pack':[0],\
                                    'start':[start],'end':[start],'type':[type(self)]}
            
    def __call__(self,index,history=False):
        if index == 1:
            temp = self.part1
        elif index == 2:
            temp = self.part2
        else:
            raise ValueError('index must be 1 or 2')
        
        print(temp.total, 'MB in partition',index)
        print(temp.occu, 'MB used in partition',index)
        print(temp.free, 'MB free in partition',index)
        print(temp.raw, 'MB used by raw data in partition',index)
        print(temp.crop, 'MB used by cropped data in partition',index)
        print(temp.proc, 'MB used by processed data in partition',index)
        print(temp.pack, 'MB used by packed data in partition',index)
        print(temp.compr, 'MB used by compressed data in partition',index)
        print(temp.cal, 'MB used by calibration data in partition',index)
        print(temp.flush, 'MB to be flushed in partition',index)

        if history:
            print('history:')
            [print(i,':',v) for i,v in temp.history.items()];


    def saving(self, index, tm_type, val, key, start, end):
        
        """
        tm is a PHI_MODE Object
        index = 1 or 2 is the partition used
        mode is mandatory for cropped datasets
        
        Warning: You must use this function after every
                 new declaration or action on PHI_MODE objects
        """
        
        def insert_values(D,tm_type,val,key,start,end):
            
            d = D.history
            
            for k in d.keys():
                if k == 'start':
                    d[k] += [start]
                elif k == 'end':
                    d[k] += [end]
                elif k == 'type':
                    d[k] += [tm_type]
                elif k == key:
                    d[k] += [val]
                elif k == 'occu':
                    if not key in ['flush','compr']:
                        d[k] += [val]
                    else:
                        d[k] += [0]
                else:
                    d[k] += [0]
            if not key in ['flush','compr']:
                D.occu += val
                D.free -= val
            if key == 'raw':
                D.raw += val
            if key == 'proc':
                D.proc += val
            if key == 'compr':
                D.compr += val
            if key == 'cal':
                D.cal += val
            if key == 'flush':
                D.flush += val
            if key == 'crop':
                D.crop += val
            if key == 'pack':
                D.pack += val

        if index == 1:
            temp = self.part1
        elif index == 2:
            temp = self.part2
        else:
            raise ValueError('index must be 1 or 2')
        
        insert_values(temp,tm_type,val,key,start,end)
        
        
    def plot(self,index, time_ordered = False, bar = True):#history=True,twin=True,bar=True):
        
        font = {'family':'DejaVu Sans',
                'weight' : 'normal',
                'size'   : 12}
        import matplotlib
        matplotlib.rc_file_defaults()
        plt.rc('font', **font)
        matplotlib.rcParams['image.origin'] = 'lower'
        
        if index == 1:
            temp = self.part1
        elif index == 2:
            temp = self.part2
        
        ty = temp.history['type']
#         xx = [ty[i] != FLUSH for i in range(len(ty))]
        
#         x0 = [temp.history['start'][i] for i in range(len(ty)) if xx[i]]
#         x1 = [temp.history['end'][i] for i in range(len(ty)) if xx[i]]
        if time_ordered:
            s = np.argsort(temp.history['start'])
        else:
            s = np.arange(np.size(temp.history['start']))
        x0 = np.asarray(temp.history['start'])[s]
        x1 = np.asarray(temp.history['end'])[s]
        
        y0 = np.asarray(temp.history['occu'])[s]
        r0 = np.asarray(temp.history['raw'])[s]
        p0 = np.asarray(temp.history['proc'])[s]
        c0 = np.asarray(temp.history['cal'])[s]
        m0 = np.asarray(temp.history['compr'])[s]
        o0 = np.asarray(temp.history['crop'])[s]
        k0 = np.asarray(temp.history['pack'])[s]

            
#         c1 = self.history['cal2']
        
#         fx0 = [temp.history['start'][i] for i in range(len(ty)) if not xx[i]]
#         fx1 = [temp.history['end'][i] for i in range(len(ty)) if not xx[i]]
        f0 = temp.history['flush']
#         f1 = temp.history['flush']
        
        fig, ax = plt.subplots(figsize=(10,6))

        a0 = ax.plot(x1, np.cumsum(y0), 'r.-', label = 'partition '+str(index))
        a1 = ax.plot(x1, np.cumsum(r0), 'g*-', label = 'raw')
#                 ax.plot(x1, np.cumsum(r1), 'g*--', label = 'raw 2')
        a2 = ax.plot(x1, np.cumsum(p0), 'bv-', label = 'processed')
#                 ax.plot(x1, np.cumsum(p1), 'bv--', label = 'processed 2')
        a3 = ax.plot(x1, np.cumsum(c0), 'ms-', label = 'calibration')
        ax.plot(x1, np.cumsum(m0), '<-', color='orange', label = 'compressed')
        ax.plot(x1, np.cumsum(o0), 'k>-', label = 'cropped')
        ax.plot(x1, np.cumsum(k0), '+-', color='pink', label = 'packed')
#                 ax.plot(x1, np.cumsum(c1), 'ms--', label = 'calibration 2')
        a4 = ax.plot(x1, np.cumsum(f0), 'c^-', alpha=1, label = 'flush')
#                 ax.plot(fx1, np.cumsum(f1), 'c^--', alpha=1, label = 'flush 2')
        tkw = dict(size=4, width=1.5)
        ax.tick_params(axis='y', **tkw)
        ax.tick_params(axis='x', **tkw)

        plt.xlabel('date')
        ax.set_ylabel('memory usage (MB)')
        plt.legend()
        fig.autofmt_xdate()
    
        if bar:
            args = {'width':(x1[-1] - x1[0]).total_seconds()/60/60/24 * 0.04, 'alpha':0.5}
            ax.bar(x1[-1], temp.raw, color='g', **args)
            ax.bar(x1[-1], temp.proc, color='b', **args, bottom = temp.raw)
            ax.bar(x1[-1], temp.cal, color='m', **args, bottom = temp.raw+temp.proc)
#                 ax.bar(x1[-1], f0[-1], 'c', **args)
            ax.bar(x1[-1], temp.compr, color='orange', **args, bottom = temp.raw+temp.proc+temp.cal)
            ax.bar(x1[-1], temp.crop, color='k', **args, bottom = temp.raw+temp.proc+temp.cal+temp.compr)
            ax.bar(x1[-1], temp.pack, color='pink', **args, bottom = temp.raw+temp.proc+temp.cal+temp.compr+temp.crop)
#                 ax.bar(x1[-1] + (x1[-1]-x1[0])/len(x1), temp.flush, color='c', **args)
        return fig
        
    def format_partition(self,index,start):
        if index == 1:
            temp = self.part1
        elif index == 2:
            temp = self.part2
        else:
            raise ValueError('index must be 1 or 2')
        
        temp.history['start'] += [start]
        temp.history['end'] += [start + datetime.timedelta(hours=3)] #TBD
        temp.history['type'] += [PHI_MEMORY.format_partition]
         
        temp.history['occu'] += [-temp.occu]
        temp.history['raw'] += [-temp.raw]
        temp.history['proc'] += [-temp.proc]
        temp.history['cal'] += [-temp.cal]
        temp.history['crop'] += [-temp.crop]
        temp.history['compr'] += [-temp.compr]
        temp.history['pack'] += [-temp.pack]
        temp.history['flush'] += [0]
        
        temp.occu = 0
        temp.raw = 0
        temp.proc = 0
        temp.cal = 0
        temp.compr = 0
        temp.crop = 0
        temp.pack = 0
        temp.free = temp.total
        
    def copy_partition(self,index,start):
        if index == 1:
            temp0 = self.part1
            temp1 = self.part2
        elif index == 2:
            temp0 = self.part2
            temp1 = self.part1
        else:
            raise ValueError('index must be 1 or 2')
        
        temp0.history['start'] += [start]
        temp0.history['end'] += [start + datetime.timedelta(hours=1)] #TBD
        temp0.history['type'] += [PHI_MEMORY.copy_partition]
        temp1.history['start'] += [start]
        temp1.history['end'] += [start + datetime.timedelta(hours=1)] #TBD
        temp1.history['type'] += [PHI_MEMORY.copy_partition]
         
        temp1.history['occu'] += [temp0.occu]
        temp1.history['raw'] += [temp0.raw]
        temp1.history['proc'] += [temp0.proc]
        temp1.history['cal'] += [temp0.cal]
        temp1.history['crop'] += [temp0.crop]
        temp1.history['compr'] += [temp0.compr]
        temp1.history['pack'] += [temp0.pack]
        temp1.history['flush'] += [0]
        
        temp0.history['occu'] += [0]
        temp0.history['raw'] += [0]
        temp0.history['proc'] += [0]
        temp0.history['cal'] += [0]
        temp0.history['crop'] += [0]
        temp0.history['compr'] += [0]
        temp0.history['pack'] += [0]
        temp0.history['flush'] += [0]
        
        temp1.occu += temp0.occu
        temp1.raw += temp0.raw
        temp1.proc += temp0.proc
        temp1.cal += temp0.cal
        temp1.compr += temp0.compr
        temp1.crop += temp0.crop
        temp1.pack += temp0.pack
        temp1.free -= temp0.occu

    def save(self,fname,overwrite = True,gui=False):

        if gui:
            if fname[-3:] == 'csv':
                df = pd.DataFrame.from_dict(self.part1.history)
                df = df.append(pd.DataFrame.from_dict(self.part2.history))
                csv = df.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                return b64
                
            else:
                raise ValueError('File format not recognized. Please use .csv')
        else:
            if fname[-3:] == 'pkl':
                if not os.path.isfile(fname):
                    with open(fname,'wb') as output:
                        pickle.dump(self,output,pickle.HIGHEST_PROTOCOL)
                else:
                    if overwrite:
                        with open(fname,'wb') as output:
                            pickle.dump(self,output,pickle.HIGHEST_PROTOCOL)
                    else:
                        raise ValueError('Cannot overwrite',fname)
            elif fname[-3:] == 'csv':
                df = pd.DataFrame.from_dict(self.part1.history)
                df = df.append(pd.DataFrame.from_dict(self.part2.history))
                if not os.path.isfile(fname):
                    df.to_csv(fname)
                else:
                    if overwrite:
                        df.to_csv(fname)
                    else:
                        raise ValueError('Cannot overwrite',fname)
            else:
                raise ValueError('File format not recognized. Please use .csv or .pkl')
  
    def _load(self,fname,gui=False):

        if gui:
            self = _load_csv(self,fname)            
        else:
            if os.path.isfile(fname):
                if fname[-3:] == 'pkl':
                    with open(fname,'rb') as handle:
                        self = pickle.load(handle)
                elif fname[-3:] == 'csv':
                    self = _load_csv(self,fname)
                else:
                    raise ValueError('File format not recognized. Please use .csv or .pkl')
            else:
                raise ValueError(fname,'not found')

def printp(a0,label=None,gui=None):
    meta = a0.raw.metadata
    tot = 0
    
    if gui == True:
        from streamlit import write
        printing = write
    else:
        printing = print

    printing(label)
    printing('number of datasets:',a0.raw.n_datasets)
    printing('cadence:', a0.raw.cadence,'mins')
    printing('duration:',a0.raw.end - a0.raw.start)
    printing('amount of raw-data at',a0.raw.n_bits,'bits:',round(a0.raw.data_tot*1e6/2**20,1), 'MiB,',round(a0.raw.data_tot*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')

    levels, _, _ = levels_did_out(a0)
    
    for l in levels:
        a1 = a0.level_out(l)
        if l.split(".")[-1] != 'compr':
            tot += a1.data_tot
        
        if l.split(".")[-1] == 'crop':
            val =a1.data_tot
            printing('crop size:',a1.X)
            printing('amount of crop-data at',a1.n_bits,'bits:',round(val*1e6/2**20,1), \
                'MiB,',round(val*1e6/2**20/a1.n_datasets,1),'MiB per dataset')
            meta += a1.metadata
            printing('cropping time:',a1.time)
        
        if l.split(".")[-1] == 'pack':
                val = a1.data_tot
                printing('amount of pack-data at',a1.n_bits,'bits:',round(val*1e6/2**20,1), \
                    'MiB,',round(val*1e6/2**20/a1.n_datasets,1),'MiB per dataset')
                meta += a1.metadata
                printing('packing time:',a1.time)

        if l.split(".")[-1] == 'proc':
                val = a1.data_tot + a1.interm_data_tot
                val_d = a1.data_tot
                nbit = a1.n_bits
                meta += a1.metadata
                printing('amount of processed data (and intermediate data) at',nbit,'bits:',round(val*1e6/2**20,1), 'MiB,',round(val_d*1e6/2**20/a1.n_datasets,1),'MiB per dataset')
                printing('processing time:',a1.time)

        if l.split(".")[-1] == 'compr':
                val = a1.data_tot
                nbit = a1.n_bits
                ndata = a1.n_datasets
                meta += a1.metadata
                printing('amount of compressed data + metadata at',nbit,'bits:',round(val*1e6/2**20,1), 'MiB,',round(val*1e6/2**20/ndata,1),'MiB per dataset')
                printing('compressing (+ flushing) time:',a1.time)
                
    printing('amount of metadata: ', meta, 'MiB')
    printing('amount of memory usage:',round((tot)*1e6/2**20,1), 'MiB')
    printing('')
    

# from matplotlib import pyplot as plt

def plot_tot(PHI,ylim=(0,250),xlim=None,time_ord=True,figp=False):
    fig,ax = plt.subplots(1,2,figsize=(15,8),sharex=True,sharey=True)
    ax[0].set_ylabel('memory usage (GB)')
    
    for axi, temp, i in zip(ax,[PHI.part1.history,PHI.part2.history],['1','2']):
        # plt.subplot(121)
        axi.set_title('Partition '+i)
        if time_ord:
            s = np.argsort(temp['start'])
        else:
            s = np.arange(np.size(temp['start']))

        y0 = np.asarray(temp['occu'])[s]
        y1 = np.asarray(temp['occu'])[s]

        x = [None]*(len(y0)+len(y1))
        o = [None]*(len(y0)+len(y1))
        r = [None]*(len(y0)+len(y1))
        c = [None]*(len(y0)+len(y1))
        
        x[::2] = np.asarray(temp['start'])[s]
        x[1::2] = np.asarray(temp['end'])[s]; del x[1]
        o[::2] = np.cumsum(y0)/1e3
        o[1::2] = np.cumsum(y1)/1e3; del o[-1]
        r[::2] = np.cumsum(np.asarray(temp['raw'])[s])/1e3
        r[1::2] = np.cumsum(np.asarray(temp['raw'])[s])/1e3; del r[-1]
        c[::2] = np.cumsum(np.asarray(temp['compr'])[s])/1e3
        c[1::2] = np.cumsum(np.asarray(temp['compr'])[s])/1e3; del c[-1]
        del y0, y1

        axi.plot_date(x,o,'ro-',label='total')
        axi.plot_date(x,r,'g*-',label='raw')
        axi.plot_date(x,c,color='orange',linestyle='-',marker='<',label='compressed')
        
        axi.grid()
        axi.axhline(220,color='k',linestyle='--',linewidth=2) #240
        
        axi.set_ylim(ylim)
        if xlim is not None:
            axi.set_xlim(xlim)
        axi.set_xlabel('date'); 
    
    fig.autofmt_xdate()
    
    if figp:
        return fig
    else:
        return None

def _load_csv(phi_memory,fname):

    phi_memory.part1 = PARTITION()
    phi_memory.part2 = PARTITION()
    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(fname, parse_dates=[9,10], date_parser=dateparse)

    indexes = df[df.type == str(PHI_MEMORY)].index

    df1 = df[:indexes[1]]
    df2 = df[indexes[1]:]

    for part, dfi in zip([phi_memory.part1,phi_memory.part2], [df1,df2]):
        

        # p = PHI_MEMORY(dfi.start[0])

        dict1 = dfi.to_dict(orient='list')
        del dict1['Unnamed: 0']
        for i,val in enumerate(dict1['start']):
            dict1['start'][i] = datetime.datetime.fromisoformat(str(val))
        for i,val in enumerate(dict1['end']):
            dict1['end'][i] = datetime.datetime.fromisoformat(str(val))


        Class_list = [PHI_MEMORY, RAW, PROC, CROP, PACK, COMPR, CAL, FLUSH, PHI_MODE, PHI_MEMORY.format_partition]
        stringClass_list = [str(s) for s in Class_list]; stringClass_list[-1] = stringClass_list[-1][:-19]
        
        for l, val in enumerate(dict1['type']):
            try:
                ind = [jj for jj,ii in enumerate(stringClass_list) if val in ii][0]
                dict1['type'][l] = Class_list[ind]
            except:
                dict1['type'][l] = Class_list[-1]

        dict1['type']

        part.history = dict1

        temp = part
        temp.occu = np.sum(temp.history['occu'])
        temp.raw = np.sum(temp.history['raw'])
        temp.proc = np.sum(temp.history['proc'])
        temp.compr = np.sum(temp.history['compr'])
        temp.pack = np.sum(temp.history['pack'])
        temp.crop = np.sum(temp.history['crop'])
        temp.flush = np.sum(temp.history['flush'])
        temp.cal = np.sum(temp.history['cal'])
        temp.free = temp.total = temp.occu

    return phi_memory

def final_plot(PHI,TM):

    from scipy.interpolate import interp1d
    tot1 = np.asarray(PHI.part1.history['occu'])
    tot2 = np.asarray(PHI.part2.history['occu'])
    c1 = np.asarray(PHI.part1.history['compr'])
    c2 = np.asarray(PHI.part2.history['compr'])
    d1 = np.asarray(PHI.part1.history['start'])
    d2 = np.asarray(PHI.part2.history['start'])

    starttime = min(d1[0],d2[0])
    endtime = max(d1[-1],d2[-1])
    t0 = starttime
    times = []
    while starttime.date() <= endtime.date():
        times.append(starttime.date())
        starttime += datetime.timedelta(days=1)
    starttime = t0; del t0
    times_float = [(t - d1[0]).total_seconds() for t in d1]
    times = np.asarray(times)

    s = np.argsort(d1)

    newtot1 = np.zeros(np.size(times))
    for i,j in zip(d1,tot1):
        if True:
            ind = np.where(times == i.date())[0]
            newtot1[ind] += j/1e3
    newtot1 = np.asarray(newtot1)

    newtot2 = np.zeros(np.size(times))
    for i,j in zip(d2,tot2):
        if True:
            ind = np.where(times == i.date())[0]
            newtot2[ind] += j/1e3
    newtot2 = np.asarray(newtot2)

    newc1 = np.zeros(np.size(times))
    for i,j in zip(d1,c1):
        if j>=0:
            ind = np.where(times == i.date())[0]
            newc1[ind] += j/1e3
    newc1 = np.asarray(newc1)


    newc2 = np.zeros(np.size(times))
    for i,j in zip(d2,c2):
        if j>=0:
            ind = np.where(times == i.date())[0]
            newc2[ind] += j/1e3
    newc2 = np.asarray(newc2)

    form1 = np.zeros(np.size(times))
    for i,j in zip(d1,c1):
        if j<0:
            ind = np.where(times == i.date())[0]
            form1[ind] += j/1e3
    form1 = np.asarray(form1)


    form2 = np.zeros(np.size(times))
    for i,j in zip(d2,c2):
        if j<0:
            ind = np.where(times == i.date())[0]
            form2[ind] += j/1e3
    form2 = np.asarray(form2)

    trig1 = np.where(form1 < 0)[0]
    trig2 = np.where(form2 < 0)[0]

    x = TM['date'][np.logical_and(TM['date'] <= endtime+datetime.timedelta(days=1),TM['date'] >= starttime-datetime.timedelta(days=1))]
    y = TM['tm_rate'][np.logical_and(TM['date'] <= endtime+datetime.timedelta(days=1),TM['date'] >= starttime-datetime.timedelta(days=1))]
    z = TM['duration'][np.logical_and(TM['date'] <= endtime+datetime.timedelta(days=1),TM['date'] >= starttime-datetime.timedelta(days=1))]

    xx = [(i - starttime).total_seconds() for i in x]
    f = interp1d(xx, y*z,fill_value="extrapolate")
    xnew = [(i - starttime.date()).total_seconds() for i in times]
    ynew = f(xnew)/8e9
    ynew[times<datetime.date(2022,4,1)] *= .2
    ynew[np.logical_and(times>=datetime.date(2022,4,1), times<datetime.date(2022,10,1))] *= .3
    ynew[times>=datetime.date(2022,10,1)] *= .2

    tm_used = np.zeros(times.size)
    down = 0#np.zeros(times.size)
    for i in range(times.size):
        if newc1[i] > 0 or newc2[i] > 0:
            tm_used[i] = min(ynew[i],np.sum(newc1[:i+1] + newc2[:i+1]) - tm_used[:i].sum())
            down = max(0,np.sum(newc1[:i+1] + newc2[:i+1] - tm_used[:i+1]))
        else:
            if down > 0:
                tm_used[i] = min(ynew[i],np.sum(newc1[:i+1] + newc2[:i+1]) - tm_used[:i].sum())
                down = max(0,np.sum(newc1[:i+1] + newc2[:i+1] - tm_used[:i+1]))

    plt.figure(figsize=(15,10))
    plt.subplot(221)
    plt.ylabel('data compression (GB)')
    plt.plot(times,newc1,label='partition 1')
    plt.plot(times,newc2,label='partition 2')
    for i,t in enumerate(trig1):
        if i == 0:
            plt.axvline(times[t],linestyle='--',color='k',alpha=.5,label='partition 1 reset')
        else:
            plt.axvline(times[t],linestyle='--',color='k',alpha=.5)
    for i,t in enumerate(trig2):
        if i == 0:
            plt.axvline(times[t],linestyle='--',color='g',alpha=.5,label='partition 2 reset')
        else:
            plt.axvline(times[t],linestyle='--',color='g',alpha=.5)
    plt.legend()

    plt.subplot(222)
    plt.ylabel('daily TM rate (GB/rate)')
    plt.plot(times,ynew,label='TM PHI rate')
    plt.plot(times,tm_used,'ro',label='TM used')
    plt.legend()
    for t in trig1:
        plt.axvline(times[t],linestyle='--',color='k',alpha=.5)
    for t in trig2:
        plt.axvline(times[t],linestyle='--',color='g',alpha=.5)
    # print('total TM used:',round(tm_used.sum(),2),'GB')
    # print('total compressed data:',round((newc1+newc2).sum(),2),'GB')
    # plt.xlim(datetime.date(2022,1,10),datetime.date(2022,2,5))
    plt.subplot(223)
    plt.ylabel('data and TM amount (GB)')
    plt.plot(times,np.cumsum(newc1+newc2),label='cumulative compressed data')
    plt.plot(times,np.cumsum(tm_used),label='cumulative TM usage')
    plt.plot(times,np.cumsum(newc1+newc2)-np.cumsum(tm_used),label='SSMM filling state')
    # cond = np.logical_and(FL['date']>datetime.datetime(2022,1,19,0,0), FL['date']<datetime.datetime(2022,5,1,0,0))
    # plt.plot(FL['date'][cond],-np.cumsum(FL['flush'][cond]*2**20/1e9),'m',label='SOOPK FLUSH')
    plt.legend()
    for t in trig1:
        plt.axvline(times[t],linestyle='--',color='k',alpha=.5)
    for t in trig2:
        plt.axvline(times[t],linestyle='--',color='g',alpha=.5)

    plt.subplot(224)
    plt.ylabel('total memory usage (GB)')
    plt.plot(times,np.cumsum(newtot1),'k',label='partition 1')
    plt.plot(times,np.cumsum(newtot2),'g',label='partition 2')
    plt.legend()
    plt.ylim(0,250)
    plt.axhline(220,linestyle='--',color='r')
    for t in trig1:
        plt.axvline(times[t],linestyle='--',color='k',alpha=.5)
    for t in trig2:
        plt.axvline(times[t],linestyle='--',color='g',alpha=.5)

    plt.gcf().autofmt_xdate()
    
    # plt.savefig('/home/calchetti/MPStemp/pics/TM/tm_v5.1.png')