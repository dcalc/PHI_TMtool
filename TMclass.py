import datetime
import numpy as np
import matplotlib.pyplot as plt
import pickle
import os
import pandas as pd

__all__ = ['PHI_MEMORY', 'PARTITION', 'RAW','PROC','CROP','PACK','COMPR','PHI_MODE','CAL','FLUSH',\
            'roundup','plot_tot','printp']


def roundup(x,base=8):
    return round(x) if x % base == 0 else round(x + base - x % base)

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
class FLUSH:
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Amount of data to be flushed: {self.data} MB')
        
    pass
class CAL:
    def __call__(self):
        print(f'Variable info: ')
        print(f'Variable type: {type(self)}')
        print(f'Start date: {self.start}')
        print(f'End date: {self.end}')
        print(f'Amount of calibration data: {self.data} MB')
        
    pass
class CROP:
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

class PHI_MODE:
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
    
    #############################################################################
    #############################################################################
    #############################################################################
        
    def observation(self,start,end,cadence,shape=(2048,2048,4,6)):
#         class RAW:
#             pass
        self.__checkMode__(['HRT','FDT'])
        self.raw = RAW()
        self.raw.cadence = cadence
        self.raw.start = start
        self.raw.end = end
        self.raw.n_bits = 32
        self.raw.X = shape[1]; self.raw.Y = shape[0]; self.raw.P = shape[2]; self.raw.L = shape[3]
        self.raw.n_pix = self.raw.X*self.raw.Y*self.raw.P*self.raw.L
        self.raw.n_outputs = self.raw.L * self.raw.P
        self.raw.n_datasets = int((self.raw.end - self.raw.start).total_seconds() / (60*self.raw.cadence))
        self.raw.this_run = self.raw.n_datasets
        # MB of raw metadata
        self.raw.metadata = 8 * self.raw.n_datasets
        # MB of raw data + metadata
        self.raw.data = roundup(self.raw.n_pix * self.raw.n_bits / 8e6) * self.raw.n_datasets #+ self.raw.metadata
        self.raw.data_tot = self.raw.data + self.raw.metadata
        
#         self.raw.memory_flag = False
        
        return {'tm_type':type(self.raw), 'val':self.raw.data_tot,\
                'key':'raw', 'start':self.raw.start, 'end':self.raw.end}
    
    #############################################################################
    #############################################################################
    #############################################################################
    
    def processing(self,start,ndata=-1,partialStore=0x00,nout=5,level='raw'):
        self.__checkMode__(['HRT','FDT'])
                 
            
        if hasattr(self,'proc'):
            if not '.' in level:
                temp = getattr(self,level)
                s = self.proc
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                if level[-1] == 'crop':
                    try:
                        s = self.proc.crop
                    except:
                        self.proc.crop = CROP()
                        s = self.proc.crop
                        s.n_datasets = 0
                        s.not_datasets = temp.n_datasets
                        s.cpu_time = datetime.timedelta(minutes=40) #TBD
                        s.interm_data_tot = 0
                        s.data_tot = 0
                        s.n_datasets = 0
                        s.n_outputs = nout
                else:
                    raise ValueError ('level not accepted (only raw and raw.crop)')
            s.start = start
            if ndata == -1:
                s.n_outputs = nout
                s.this_run = temp.n_datasets - self.proc.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= s.not_datasets:
                s.n_datasets += ndata
                s.not_datasets -= ndata
                s.this_run = ndata
            elif ndata > s.not_datasets:
                s.this_run = temp.n_datasets - s.n_datasets
                print(f'Exceeding the number of datasets, ndata set to {s.not_datasets}')
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(s.not_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets -= s.this_run
                
        else:
            if not '.' in level:
                temp = getattr(self,level)
                self.proc = PROC()
                s = self.proc
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                if level[-1] == 'crop':
                    self.proc = PROC()
                    self.proc.crop = CROP()
                    s = self.proc.crop
                else:
                    raise ValueError ('level not accepted (only raw and raw.crop)')
            
            s.start = start
            s.cpu_time = datetime.timedelta(minutes=40) #TBD
            s.n_outputs = nout
            s.interm_data_tot = 0
            s.data_tot = 0
            s.n_datasets = 0
            if ndata == -1:
                s.this_run = temp.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= temp.n_datasets:
                s.this_run = ndata
                s.n_datasets = ndata
                s.not_datasets = temp.n_datasets - ndata
            elif ndata > temp.n_datasets:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += self.proc.this_run
                s.not_datasets = temp.n_datasets - s.this_run
        
        s.end = s.start + s.cpu_time * s.this_run
        # intermediate processing steps
        if partialStore == 0x00:
            s.interm_steps = 1
        if partialStore == 0xFF:
            s.interm_steps = 5
        s.interm_n_bits = 32
        s.n_bits = 16
        
        
        try:
            s.crop_x = temp.crop_x
            s.crop_y = temp.crop_y
        except:
            s.crop_x = self.raw.X
            s.crop_y = self.raw.Y
    
        #MB of intermediate data + metadata
        s.interm_metadata = 8 * s.interm_steps * s.this_run
        s.interm_data = roundup(round(temp.n_outputs*s.crop_x*s.crop_y,0)*\
                                 s.interm_n_bits / 8e6) * s.interm_steps * s.this_run
        s.interm_data_tot += s.interm_data + s.interm_metadata

        #MB of processed data + metadata
        s.metadata = 8 * s.this_run
        s.data = roundup(round(s.crop_x*s.crop_y,0)*\
                          s.n_bits / 8e6 * s.n_outputs) * s.this_run
        s.data_tot += s.data + s.metadata

        return {'tm_type':PROC, 'val':s.data + s.metadata + s.interm_data + s.interm_metadata,\
                'key':'proc', 'start':s.start, 'end':s.end}
    
    
    
    
    
    #############################################################################
    #############################################################################
    #############################################################################
    
    def compressing(self,start,nbits,ndata,level = 'proc'):
#         class COMPR:
#             pass
        self.__checkMode__(['HRT','FDT'])
        if not hasattr(self,'raw'):
            raise ValueError('You must run .observation() and .processing() before compressing! Bye bye.')
        if not hasattr(self,'proc'):
            if not hasattr(self.raw,'pack'):
                raise ValueError('You must run .processing() before compressing! Bye bye.')
        
        if hasattr(self,'compr'):
            
            if not '.' in level:
                temp = getattr(self,level)
                s = self.compr
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                if level[-1] == 'crop':
                    try:
                        s = self.compr.crop
                    except:
                        self.compr.crop = CROP()
                        s = self.compr.crop
                        s.n_datasets = 0
                        s.not_datasets = temp.n_datasets
                        s.cpu_time = datetime.timedelta(minutes=1) #TBD
                        s.data_tot = 0
                        s.n_datasets = 0
                        
                elif level[-1] == 'pack':
                    try:
                        s = self.compr.pack
                    except:
                        self.compr.pack = PACK()
                        s = self.compr.pack
                        s.n_datasets = 0
                        s.not_datasets = temp.n_datasets
                        s.cpu_time = datetime.timedelta(minutes=1) #TBD
                        s.data_tot = 0
                        s.n_datasets = 0
                else:
                    raise ValueError ('level not accepted')
                
            s.start = start
            s.n_outputs = temp.n_outputs
            max_data = temp.n_datasets - s.n_datasets
            if ndata == -1:
                s.this_run = temp.n_datasets - self.proc.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= max_data:
                s.n_datasets += ndata
                s.not_datasets -= ndata
                s.this_run = ndata
            elif ndata > max_data:
                s.this_run = temp.n_datasets - s.n_datasets
                print(f'Exceeding the number of datasets, ndata set to {max_data}')
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(s.not_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets -= s.this_run
                
        else:
            if not '.' in level:
                temp = getattr(self,level)
                self.compr = COMPR()
                s = self.compr
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                if level[-1] == 'crop':
                    self.compr = COMPR()
                    self.compr.crop = CROP()
                    s = self.compr.crop
                    
                elif level[-1] == 'pack':
                    self.compr = COMPR()
                    self.compr.pack = PACK()
                    s = self.compr.pack
                else:
                    raise ValueError ('level not accepted (only proc, proc.crop, proc.pack and raw.proc)')
            
            s.start = start
            s.n_outputs = temp.n_outputs
            s.cpu_time = datetime.timedelta(minutes=1) #TBD
            s.data_tot = 0
            s.n_datasets = 0
            if ndata == -1:
                s.this_run = temp.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= temp.n_datasets:
                s.this_run = ndata
                s.n_datasets = ndata
                s.not_datasets = temp.n_datasets - ndata
            elif ndata > temp.n_datasets:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += self.proc.this_run
                s.not_datasets = temp.n_datasets - s.this_run
        
        s.end = s.start + s.cpu_time * s.this_run
        s.n_bits = nbits
        if hasattr(temp,'crop_x'):
            s.crop_x = temp.crop_x
            s.crop_y = temp.crop_y
            
        else:
            s.crop_x = self.raw.X
            s.crop_y = self.raw.Y
            
        #MB of compressed data + metadata
        
        if 'raw' in level:
            s.data = (round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits / 8e6 * s.n_outputs + 0.7) * s.this_run
        else:
            s.data = (round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits / 8e6 * s.n_outputs + 0.7) * s.this_run
        
        s.data_tot += s.data

        return {'tm_type':COMPR, 'val':s.data,\
                'key':'compr', 'start':s.start, 'end':s.end}
    
    
    #############################################################################
    #############################################################################
    #############################################################################
        
    def cropping(self,start,crop,ndata=-1,level='raw'):
        self.__checkMode__(['HRT','FDT'])
        if not hasattr(self,'raw'):
            raise ValueError('You must run at least .observation() before cropping! Bye bye.')
#         try:
#             temp = getattr(self,level)
#         except:
#             raise ValueError(f'The selected level ({level}) is not defined yet')
                
        if isinstance(crop,int):
            crop_x = crop
            crop_y = crop
        elif isinstance(crop,list):
            if len(crop) == 2:
                crop_x = crop[1]
                crop_y = crop[0]
        else:
            raise ValueError('crop variable must be an integer or a list with two integer elements (now only power of 2 are accepted)')
            
        try:
            if not '.' in level:
                temp = getattr(self,level)

            else:
                l = level.split('.')
                temp = getattr(self,l[0])
        except:
            raise ValueError(f'The selected level ({level}) is not defined yet')
        
        if hasattr(temp,'crop'):
            if not '.' in level:
#                 temp = getattr(self,level)
                s = temp.crop
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                s = getattr(self,level[0]).crop
              
            s.start = start
            max_data = temp.n_datasets - s.n_datasets
            s.crop_x = crop_x
            s.crop_y = crop_y
            if ndata == -1:
                s.this_run = max_data
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= max_data:
                s.this_run = ndata
                s.n_datasets += ndata
                s.not_datasets = s.n_datasets - ndata
            elif ndata > max_data:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets = temp.n_datasets - s.n_datasets     
        else:
            if not '.' in level:
                temp = getattr(self,level)
                temp.crop = CROP()
                s = temp.crop
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                getattr(self,level[0]).crop = CROP()
                s = getattr(self,level[0]).crop
                        
            s.level = level
            s.start = start
            s.crop_x = crop_x
            s.crop_y = crop_y
            s.cpu_time = datetime.timedelta(minutes=1) #TBD
            s.n_outputs = temp.n_outputs
            s.data_tot = 0
            s.n_datasets = 0
            if ndata == -1:
                s.this_run = temp.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= temp.n_datasets:
                s.this_run = ndata
                s.n_datasets = ndata
                s.not_datasets = s.n_datasets - ndata
            elif ndata > temp.n_datasets:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets = s.n_datasets - s.this_run
        
        s.end = s.start + s.cpu_time * s.this_run
        
        s.n_bits = temp.n_bits
        if 'pack' in level:
            s.n_bits = 16
        
        #MB of processed data + metadata
        
        if 'raw' in level:
            s.metadata = 8 * s.this_run
            s.data = roundup(round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits / 8e6 * s.n_outputs) * s.this_run
        else:
            s.metadata = 8 * s.this_run
            s.data = roundup(round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits * s.n_outputs / 8e6)* s.this_run
        
        s.data_tot += s.data + s.metadata
        return {'tm_type':CROP, 'val':s.data + s.metadata,\
                'key':'crop', 'start':s.start, 'end':s.end}
    
    
    #############################################################################
    #############################################################################
    #############################################################################
     
        
    def packing(self,start,ndata=-1,level='raw'):
        self.__checkMode__(['HRT','FDT'])
        if not hasattr(self,'raw'):
            raise ValueError('You must run at least .observation() before cropping! Bye bye.')
                   
        try:
            if not '.' in level:
                temp = getattr(self,level)

            else:
                l = level.split('.')
                temp = getattr(self,l[0])
        except:
            raise ValueError(f'The selected level ({level}) is not defined yet')
        
        if hasattr(temp,'pack'):
            if not '.' in level:
#                 temp = getattr(self,level)
                s = temp.pack
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                s = getattr(self,level[0]).pack
              
            s.start = start
            max_data = temp.n_datasets - s.n_datasets
            s.n_bits = 16
#             s.crop_x = crop_x
#             s.crop_y = crop_y
            if ndata == -1:
                s.this_run = max_data
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= max_data:
                s.this_run = ndata
                s.n_datasets += ndata
                s.not_datasets = s.n_datasets - ndata
            elif ndata > max_data:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets = temp.n_datasets - s.n_datasets     
        else:
            if not '.' in level:
                temp = getattr(self,level)
                temp.pack = PACK()
                s = temp.pack
            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                getattr(self,level[0]).pack = PACK()
                s = getattr(self,level[0]).pack
                        
            s.level = level
            s.start = start
            s.n_bits = 16
            s.cpu_time = datetime.timedelta(minutes=1) #TBD
            s.n_outputs = temp.n_outputs
            s.data_tot = 0
            s.n_datasets = 0
            if ndata == -1:
                s.this_run = temp.n_datasets
                s.n_datasets = temp.n_datasets
                s.not_datasets = 0
            elif ndata <= temp.n_datasets:
                s.this_run = ndata
                s.n_datasets = ndata
                s.not_datasets = s.n_datasets - ndata
            elif ndata > temp.n_datasets:
                print(f'Exceeding the number of datasets, ndata set to {temp.n_datasets}')
                s.n_datasets = temp.n_datasets
                s.this_run = temp.n_datasets
                s.not_datasets = 0
            elif type(ndata) == datetime.datetime:
                s.end = ndata
                s.this_run = min(temp.n_datasets,int(ndata-start)/s.cpu_time)
                s.n_datasets += s.this_run
                s.not_datasets = s.n_datasets - s.this_run
        
        s.end = s.start + s.cpu_time * s.this_run
        
        if hasattr(temp,'crop_x'):
            s.crop_x = temp.crop_x
            s.crop_y = temp.crop_y
            
        else:
            s.crop_x = self.raw.X
            s.crop_y = self.raw.Y
                
        #MB of packed data + metadata
        
        if 'raw' in level:
            s.metadata = 8 * s.this_run
            s.data = roundup(round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits / 8e6 * s.n_outputs) * s.this_run
        else:
            s.metadata = 8 * s.this_run
            s.data = roundup(round(s.crop_x*s.crop_y,0)*\
                                 s.n_bits * s.n_outputs / 8e6) * s.this_run
        
        s.data_tot += s.data + s.metadata
        return {'tm_type':PACK, 'val':s.data + s.metadata,\
                'key':'pack', 'start':s.start, 'end':s.end}
    
    
    
    #############################################################################
    #############################################################################
    #############################################################################
    
    # def extract(self,start,level = 'raw'):
    #     self.__checkMode__(['HRT','FDT'])
    #     if not hasattr(self,'raw'):
    #         raise ValueError('You must run at least .observation() before extracting! Bye bye.')
    #     try:
    #         if not '.' in level:
    #             new = getattr(self,level)

    #         else:
    #             level = level.split('.')
    #             new = self
    #             for l in level:
    #                 new = getattr(new,l)
    #     except:
    #         raise ValueError(f'The selected level ({level}) is not defined yet')
        
    #     new.data = new.data / new.this_run / new.n_outputs
    #     new.n_datasets = 1
    #     new.this_run = 1
    #     new.not_dataset = 0
    #     new.n_outputs = 1
    #     new.start = start
    #     new.cpu_time = datetime.timedelta(minutes=2) #TBD
    #     new.end = new.start + new.cpu_time * new.this_run
        
    #     if type(new) == RAW:
    #         temp = PHI_MODE('HRT')
    #         temp.raw = new
    #     elif type(new) == PROC:
    #         temp = PHI_MODE('HRT')
    #         temp.proc = new
    #     elif type(new) == COMPR:
    #         temp = PHI_MODE('HRT')
    #         temp.compr = new
    #     elif type(new) == CROP:
    #         temp = PHI_MODE('HRT')
    #         temp.crop = new
    #     elif type(new) == PACK:
    #         temp = PHI_MODE('HRT')
    #         temp.pack = new
        
    #     return (temp, {'tm_type':type(new), 'val':new.data,\
    #             'key':'proc', 'start':new.start, 'end':new.end})
    
    def extract(self,start,nimage = 1, level = 'raw'):
        self.__checkMode__(['HRT','FDT'])
        if not hasattr(self,'raw'):
            raise ValueError('You must run at least .observation() before extracting! Bye bye.')
        try:
            if not '.' in level:
                temp = getattr(self,level)
                lev = level

            else:
                level = level.split('.')
                temp = self
                for l in level:
                    temp = getattr(temp,l)
                lev = level[-1]
        except:
            raise ValueError(f'The selected level ({level}) is not defined yet')
        
        temp.start = start
        temp.end = temp.start + datetime.timedelta(minutes=.5) * nimage #TBD
        temp.metadata = 8 * nimage
        temp.data_tot += temp.metadata
        return {'tm_type':type(temp), 'val':temp.metadata,\
                'key':lev, 'start':temp.start, 'end':temp.end}
    
    #############################################################################
    #############################################################################
    #############################################################################
     

    def flushing(self,start,end,data_vol):
        self.__checkMode__('FLUSH')
        self.flush = FLUSH()
        self.flush.start = start
        self.flush.end = end
        self.flush.data = data_vol #MB
        
        return {'tm_type':type(self.flush), 'val':self.flush.data,\
                'key':'flush', 'start':self.flush.start, 'end':self.flush.end}
    
        
        
    def calibration(self,start,end,data_vol,insert='data_vol'):
        self.__checkMode__('CAL')
        self.cal = CAL()
        self.cal.start = start
        self.cal.end = end
        
        if insert == 'data_vol':
            self.cal.data = data_vol #MB
            return {'tm_type':type(self.cal), 'val':self.cal.data,\
                'key':'cal', 'start':self.cal.start, 'end':self.cal.end}
        
        if insert == 'n_datasets':
            n_datasets = data_vol
            data_vol = roundup(2048*2048*24*32/8e6 *(32+16)/32) * n_datasets
            compr_vol = (2048*2048*24*6/8e6 + 0.7) * n_datasets
            self.cal.data = data_vol #MB
            self.cal.metadata = 8 * n_datasets
            return ({'tm_type':type(self.cal), 'val':self.cal.data + self.cal.metadata,\
                'key':'cal', 'start':self.cal.start, 'end':self.cal.end},
                   {'tm_type':type(self.cal), 'val':compr_vol,\
                'key':'compr', 'start':self.cal.start, 'end':self.cal.end})
        
        

class PHI_MEMORY:
    import matplotlib.pyplot as plt
    import numpy as np
    
    def __init__(self,start):
        
        if type(start) == list or type(start) == str:
            self._load(start)
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
        temp.history['end'] += [start + datetime.timedelta(hours=1)] #TBD
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

    def save(self,fname,overwrite = True):
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

    
    def _load(self,fname):
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

def printp(a0,gui=None):
    meta = a0.raw.metadata
    tot = a0.raw.data_tot
    
    if gui == True:
        from streamlit import write
        printing = write
    else:
        printing = print

    printing('number of datasets:',a0.raw.n_datasets)
    printing('cadence:', a0.raw.cadence,'mins')
    printing('duration:',a0.raw.end - a0.raw.start)
    printing('amount of raw-data at',a0.raw.n_bits,'bits:',round(a0.raw.data*1e6/2**20,1), 'MiB,',round(a0.raw.data*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')

    if hasattr(a0.raw,'crop'):
        val = a0.raw.crop.data
        printing('amount of crop-data at',a0.raw.n_bits,'bits:',round(val*1e6/2**20,1), \
              'MiB,',round(val*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')
        meta += a0.raw.crop.metadata
        tot += a0.raw.crop.data_tot
        
    
    if hasattr(a0.raw,'pack'):
        val = a0.raw.pack.data
        printing('amount of pack-data at',a0.raw.pack.n_bits,'bits:',round(val*1e6/2**20,1), \
              'MiB,',round(val*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')
        meta += a0.raw.pack.metadata
        tot += a0.raw.pack.data_tot
        

    if hasattr(a0,'proc'):
        if hasattr(a0.proc,'crop'):
            val = a0.proc.crop.data + a0.proc.crop.interm_data
            val_d = a0.proc.crop.data
            nbit = a0.proc.crop.n_bits
            meta += a0.proc.crop.metadata
            tot += a0.proc.crop.data_tot + a0.proc.crop.interm_data_tot
            
        else:
            val = a0.proc.data + a0.proc.interm_data
            val_d = a0.proc.data
            nbit = a0.proc.n_bits
            meta += a0.proc.metadata
            tot += a0.proc.data_tot + a0.proc.interm_data_tot
            
        printing('amount of processed data (and intermediate data) at',nbit,'bits:',round(val*1e6/2**20,1), 'MiB,',round(val_d*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')
    

    if hasattr(a0.compr,'crop'):
        val = a0.compr.crop.data
        nbit = a0.compr.crop.n_bits
        # meta += a0.compr.crop.metadata
        # tot += a0.compr.crop.data_tot
        # printing('tot_step5:', tot*1e6/2**20)
    elif hasattr(a0.compr,'pack'):
        val = a0.compr.pack.data
        nbit = a0.compr.pack.n_bits
        # meta += a0.compr.pack.metadata
        # tot += a0.compr.pack.data_tot
        # printing('tot_step6:', tot*1e6/2**20)
    else:
        val = a0.compr.data
        nbit = a0.compr.n_bits
        # meta += a0.compr.metadata
        # tot += a0.compr.data_tot
        # printing('tot_step7:', tot*1e6/2**20)
    printing('amount of compressed data + metadata at',nbit,'bits:',round(val*1e6/2**20,1), 'MiB,',round(val*1e6/2**20/a0.raw.n_datasets,1),'MiB per dataset')
    printing('amount of metadata: ', meta, 'MiB')
    printing('amount of memory usage:',round((tot)*1e6/2**20,1), 'MiB')
    
    

# from matplotlib import pyplot as plt

def plot_tot(PHI,ylim=(0,250),xlim=None,figp=False):
    fig,ax = plt.subplots(1,2,figsize=(15,8),sharex=True,sharey=True)
    ax[0].set_ylabel('memory usage (GB)')

    for axi, temp, i in zip(ax,[PHI.part1.history,PHI.part2.history],['1','2']):
        # plt.subplot(121)
        axi.set_title('Partition '+i)
        
        y0 = temp['occu']
        y1 = temp['occu']

        x = [None]*(len(y0)+len(y1))
        o = [None]*(len(y0)+len(y1))
        r = [None]*(len(y0)+len(y1))
        c = [None]*(len(y0)+len(y1))
        
        x[::2] = temp['start']
        x[1::2] = temp['end']; del x[1]
        o[::2] = np.cumsum(y0)/1e3
        o[1::2] = np.cumsum(y1)/1e3; del o[-1]
        r[::2] = np.cumsum(temp['raw'])/1e3
        r[1::2] = np.cumsum(temp['raw'])/1e3; del r[-1]
        c[::2] = np.cumsum(temp['compr'])/1e3
        c[1::2] = np.cumsum(temp['compr'])/1e3; del c[-1]
        del y0, y1

        axi.plot_date(x,o,'ro-',label='total')
        axi.plot_date(x,r,'g*-',label='raw')
        axi.plot_date(x,c,color='orange',linestyle='-',marker='<',label='compressed')
        
        axi.grid()
        axi.axhline(240,color='k',linestyle='--',linewidth=2)
        
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
