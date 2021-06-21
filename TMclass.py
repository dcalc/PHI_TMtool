import datetime
import numpy as np
import matplotlib.pyplot as plt

class RAW:
    pass
class PROC:
    pass
class COMPR:
    pass
class FLUSH:
    pass
class CAL:
    pass
class CROP:
    pass
class PACK:
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
        self.raw.data = (self.raw.n_pix * self.raw.n_bits * self.raw.n_datasets) / 1e6 / 8 + self.raw.metadata
        self.raw.data_tot = self.raw.data
        
#         self.raw.memory_flag = False
        
        return {'tm_type':type(self.raw), 'val':self.raw.data,\
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
            s.crop_x = 1
            s.crop_y = 1
    
        #MB of intermediate data + metadata
        s.interm_data_tot += (round(self.raw.X*self.raw.Y*temp.n_outputs/s.crop_x/s.crop_y,0)*\
                                 s.interm_n_bits / 8e6 + 8) * s.interm_steps * s.this_run
        s.interm_data = (round(self.raw.X*self.raw.Y*temp.n_outputs/s.crop_x/s.crop_y,0)*\
                                 s.interm_n_bits / 8e6 + 8) * s.interm_steps * s.this_run
        
        #MB of processed data + metadata
        s.data_tot += (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                                 s.n_bits / 8e6 + 8) * s.n_outputs * s.this_run
        s.data = (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                          s.n_bits / 8e6 + 8) * s.n_outputs * s.this_run
        
        return {'tm_type':type(s), 'val':s.data + s.interm_data,\
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
                        s.cpu_time = datetime.timedelta(minutes=33) #TBD
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
                        s.cpu_time = datetime.timedelta(minutes=33) #TBD
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
            s.cpu_time = datetime.timedelta(minutes=33) #TBD
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
            s.crop_x = 1
            s.crop_y = 1
            
        #MB of compressed data + metadata
        norm_meta = 1
        if 'raw' in level:
            norm_meta = 1/s.n_outputs
        s.data_tot += (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                                 s.n_bits / 8e6 + 0.1 * norm_meta) * s.n_outputs * s.this_run
        s.data = (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                                 s.n_bits / 8e6 + 0.1 * norm_meta) * s.n_outputs * s.this_run
        
        return {'tm_type':type(s), 'val':s.data,\
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
            s.cpu_time = datetime.timedelta(minutes=20) #TBD
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
        norm_meta = 1
        if 'raw' in level:
            norm_meta = 1/s.n_outputs
        s.data_tot += (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                                 s.n_bits / 8e6 + 8 * norm_meta) * s.n_outputs * s.this_run
        s.data = (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                          s.n_bits / 8e6 + 8 * norm_meta) * s.n_outputs * s.this_run
        
        return {'tm_type':type(temp.crop), 'val':s.data,\
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
            s.cpu_time = datetime.timedelta(minutes=20) #TBD
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
            s.crop_x = 1
            s.crop_y = 1
                
        #MB of packed data + metadata
        norm_meta = 1
        if 'raw' in level:
            norm_meta = 1/s.n_outputs
        s.data_tot += (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                                 s.n_bits / 8e6 + 8 * norm_meta) * s.n_outputs * s.this_run
        s.data = (round(self.raw.X*self.raw.Y/s.crop_x/s.crop_y,0)*\
                          s.n_bits / 8e6 + 8 * norm_meta) * s.n_outputs * s.this_run
        
        return {'tm_type':type(s), 'val':s.data,\
                'key':'pack', 'start':s.start, 'end':s.end}
    
    
    
    #############################################################################
    #############################################################################
    #############################################################################
    
    def extract(self,start,level = 'raw'):
        self.__checkMode__(['HRT','FDT'])
        if not hasattr(self,'raw'):
            raise ValueError('You must run at least .observation() before packing! Bye bye.')
#         try:
#             temp = getattr(self,level)
#         except:
#             raise ValueError(f'The selected level ({level}) is not defined yet')
        
#         new = PHI_MODE(self.mode)
        try:
            if not '.' in level:
                new = getattr(self,level)

            else:
                level = level.split('.')
                new = self
                for l in level:
                    new = getattr(new,l)
        except:
            raise ValueError(f'The selected level ({level}) is not defined yet')
        
        new.data = new.data / new.this_run / new.n_outputs
        new.n_datasets = 1
        new.this_run = 1
        new.not_dataset = 0
        new.n_outputs = 1
        new.start = start
        new.cpu_time = datetime.timedelta(minutes=2) #TBD
        new.end = new.start + new.cpu_time * new.this_run
        
        if type(new) == RAW:
            temp = PHI_MODE('HRT')
            temp.raw = new
        elif type(new) == PROC:
            temp = PHI_MODE('HRT')
            temp.proc = new
        elif type(new) == COMPR:
            temp = PHI_MODE('HRT')
            temp.compr = new
        elif type(new) == CROP:
            temp = PHI_MODE('HRT')
            temp.crop = new
        elif type(new) == PACK:
            temp = PHI_MODE('HRT')
            temp.pack = new
        
        return (temp, {'tm_type':type(new), 'val':new.data,\
                'key':'proc', 'start':new.start, 'end':new.end})
    
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
    
        
        
    def calibration(self,start,end,data_vol):
        self.__checkMode__('CAL')
        self.cal = CAL()
        self.cal.start = start
        self.cal.end = end
        self.cal.data = data_vol #MB
        
        return {'tm_type':type(self.cal), 'val':self.cal.data,\
                'key':'cal', 'start':self.cal.start, 'end':self.cal.end}
    
        

class PHI_MEMORY:
    import matplotlib.pyplot as plt
    import numpy as np
    
    def __init__(self,start):
        class PARTITION:
            pass
        
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
                    if not key in 'flush':
                        d[k] += [val]
                    else:
                        d[k] += [0]
                else:
                    d[k] += [0]
            if not key in 'flush':
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
        
        
    def plot(self,index,history = True, bar = False):#history=True,twin=True,bar=True):
        
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
        x0 = temp.history['start']
        x1 = temp.history['end']
        y0 = temp.history['occu']
#         y1 = self.history['occu2']
        r0 = temp.history['raw']
#         r1 = self.history['raw2']
        p0 = temp.history['proc']
#         p1 = self.history['proc2']
        c0 = temp.history['cal']
        m0 = temp.history['compr']
        o0 = temp.history['crop']
        k0 = temp.history['pack']
#         c1 = self.history['cal2']
        
#         fx0 = [temp.history['start'][i] for i in range(len(ty)) if not xx[i]]
#         fx1 = [temp.history['end'][i] for i in range(len(ty)) if not xx[i]]
        f0 = temp.history['flush']
#         f1 = temp.history['flush']
        
        if history:
            fig, ax = plt.subplots(figsize=(10,8))

            a0 = ax.plot(x1, np.cumsum(y0), 'r.-', label = 'partition '+str(index))
#             ax.plot(x1, np.cumsum(y1), 'r.--', label = 'partition 2')

#             if twin:
#                 twin1 = ax.twinx()
#                 twin2 = ax.twinx()
#                 twin3 = ax.twinx()
#                 twin4 = ax.twinx()

#                 twin1.spines['left'].set_position(("axes", -.1))
#                 twin1.yaxis.set_label_position('left')
#                 twin1.yaxis.set_ticks_position('left')

#                 twin2.spines['left'].set_position(("axes", -.2))
#                 twin2.yaxis.set_label_position('left')
#                 twin2.yaxis.set_ticks_position('left')

#                 twin3.spines['left'].set_position(("axes", -.3))
#                 twin3.yaxis.set_label_position('left')
#                 twin3.yaxis.set_ticks_position('left')
#                 twin4.spines['left'].set_position(("axes", -.4))
#                 twin4.yaxis.set_label_position('left')
#                 twin4.yaxis.set_ticks_position('left')
#                 twin1.set_ylabel('Raw data usage (MiByte)')
#                 twin2.set_ylabel('Processed data usage (MiByte)')
#                 twin3.set_ylabel('Calibration data usage (MiByte)')
#                 twin4.set_ylabel('Flush (MiByte)')

#                 a1 = twin1.plot(x1, np.cumsum(r0), 'g*-', label = 'raw 1')
#                 twin1.plot(x1, np.cumsum(r1), 'g*--', label = 'raw 2')
#                 a2 = twin2.plot(x1, np.cumsum(p0), 'bv-', label = 'processed 1')
#                 twin2.plot(x1, np.cumsum(p1), 'bv--', label = 'processed 2')
#                 a3 = twin3.plot(x1, np.cumsum(c0), 'ms-', label = 'calibration 1')
#                 twin3.plot(x1, np.cumsum(c1), 'ms--', label = 'calibration 2')
#                 a4 = twin4.plot(fx1, np.cumsum(f0), 'c^-', alpha=1, label = 'flush 1')
#                 twin4.plot(fx1, np.cumsum(f1), 'c^--', alpha=1, label = 'flush 2')
#                 ax.yaxis.label.set_color('red')
#                 twin1.yaxis.label.set_color('green')
#                 twin2.yaxis.label.set_color('blue')
#                 twin3.yaxis.label.set_color('magenta')
#                 twin4.yaxis.label.set_color('cyan')
#                 tkw = dict(size=4, width=1.5)
#                 ax.tick_params(axis='y', colors='red', **tkw)
#                 twin1.tick_params(axis='y', colors='green', **tkw)
#                 twin2.tick_params(axis='y', colors='blue', **tkw)
#                 twin3.tick_params(axis='y', colors='magenta', **tkw)
#                 twin4.tick_params(axis='y', colors='cyan', **tkw)
#                 ax.tick_params(axis='x', **tkw)
#             else:
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
                ax.bar(x1[-1], temp.raw, color='g', alpha = .5)
                ax.bar(x1[-1], temp.proc, color='b', alpha = .5, bottom = temp.raw)
                ax.bar(x1[-1], temp.cal, color='m', alpha = .5, bottom = temp.raw+temp.proc)
#                 ax.bar(x1[-1], f0[-1], 'c', alpha = .5)
                ax.bar(x1[-1], temp.compr, color='orange', alpha = .5, bottom = temp.raw+temp.proc+temp.cal)
                ax.bar(x1[-1], temp.crop, color='k', alpha = .5, bottom = temp.raw+temp.proc+temp.cal+temp.compr)
                ax.bar(x1[-1], temp.pack, color='pink', alpha = .5, bottom = temp.raw+temp.proc+temp.cal+temp.compr+temp.crop)
#                 ax.bar(x1[-1] + (x1[-1]-x1[0])/len(x1), temp.flush, color='c', alpha = .5)
            
#         if bar:
#             N = 4
#             t0 = [phi.proc1, 0, phi.proc2, 0]
#             t1 = [phi.cal1, 0, phi.cal2, 0]
#             t2 = [phi.raw1, 0, phi.raw2, 0]
#             f0 = [0, phi.flush1, 0, phi.flush2]
            
#             ind = np.arange(N) # the x locations for the groups
#             width = 0.35
#             fig, ax = plt.subplots(figsize=(6,8))
#             ax.set_title(str(phi.history['stop'][-1]))
            
#             ax.bar(ind, t0, width, color='b')
#             b = t0
#             ax.bar(ind, t1, width, bottom= b, color='m')
#             b = [t0[0]+t1[0],t0[1]+t1[1],t0[2]+t1[2],t0[3]+t1[3]]
#             ax.bar(ind, t2, width, bottom= b, color='g')
#             ax.bar(ind, f0, width, color='c')
#             ax.set_ylabel('memory usage (MiByte)')
# #             ax.set_title('Scores by group and gender')
#             ax.set_xticks(ind)
#             ax.set_xticklabels(['partition '+str(index), 'raw ', \
#                                 'processed '+str(index), 'compressed ',\
#                                 'calibration ', 'flush '])
#             # ax.set_yticks(np.arange(0, 81, 10))
            # ax.legend(labels=['Men', 'Women'])
#             fig.show()
        
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
        temp.free -= temp0.occu

