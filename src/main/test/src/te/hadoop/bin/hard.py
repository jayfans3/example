#!/usr/bin/env python

import time,sys,os

class hardInfo():
    counter=0
    sleep_time=1
    log_to_file=0
    log_path=''
    except_0=0
    def read_cpu_usage(self):         
        lines = open("/proc/stat").readlines() 
        list_cpu_info={}
        for line in lines:          
            cpu_info = line.split() 
            if len(cpu_info) < 5: 
                continue 
            if cpu_info[0].startswith('cpu'): 
                list_cpu_info[cpu_info[0]]= cpu_info
        return list_cpu_info
    def cpu_usage_draw(self,pre_cpu_info,end_cpu_info):    
            end_cpu_info= self.read_cpu_usage()
            for key in pre_cpu_info:
                pre_cpu_all=long(pre_cpu_info[key][1])+long(pre_cpu_info[key][2])+\
                float(pre_cpu_info[key][3])+long(pre_cpu_info[key][5])+\
                long(pre_cpu_info[key][6])+long(pre_cpu_info[key][7])+\
                long(pre_cpu_info[key][4])
                
                end_cpu_all=long(end_cpu_info[key][1])+long(end_cpu_info[key][2])+\
                float(end_cpu_info[key][3])+long(end_cpu_info[key][5])+\
                long(end_cpu_info[key][6])+long(end_cpu_info[key][7])+\
                long(end_cpu_info[key][4])
                
                pre_cpu_user=long(pre_cpu_info[key][1])+long(pre_cpu_info[key][2])
                end_cpu_user=long(end_cpu_info[key][1])+long(end_cpu_info[key][2])
                
                pre_cpu_idle=long(pre_cpu_info[key][4])
                end_cpu_idle=long(end_cpu_info[key][4])
                
                pre_cpu_io=long(pre_cpu_info[key][5])
                end_cpu_io=long(end_cpu_info[key][5])
                try:
                    cpu_user=round((end_cpu_user - pre_cpu_user)/(end_cpu_all - pre_cpu_all)*100)        
                    cpu_idel=round((end_cpu_idle - pre_cpu_idle)/(end_cpu_all - pre_cpu_all)*100)        
                    cpu_io=round((end_cpu_io - pre_cpu_io)/(end_cpu_all - pre_cpu_all)*100)        
                    cpu_sys=100-cpu_io-cpu_idel-cpu_user
                except:
                    cpu_idel=100
                print(key.ljust(6)+'|'+'U'*int(cpu_user)+'|'+'W'*int(cpu_io)+'|'+'S'*int(cpu_sys)+'|'+' '*int(cpu_idel)+'|')                 
                
                if self.log_to_file==1:
                    fhadle=open(self.log_path+'/cpu.log','a')
                    fhadle.writelines(str(self.counter)+','+key.ljust(6)+','+str(cpu_user)+','+str(cpu_io)+','+str(cpu_sys)+','+str(cpu_idel)+'\r')
                    fhadle.close()
                else:
                    pass
                
                
    def read_disk_usage(self):         
        lines = open("/proc/diskstats").readlines() 
        list_disk_info={}
        for line in lines:          
            cpu_info = line.split() 
            if len(cpu_info) < 5: 
                continue 
            if cpu_info[2].startswith('sd'): 
                list_disk_info[cpu_info[2]]= cpu_info
        return list_disk_info
    
    def disk_usage_draw(self,pre_disk_info,end_disk_info):    
            end_disk_info= self.read_disk_usage()
            for key in pre_disk_info:
               
                pre_disk_read=pre_disk_info[key][5]
                pre_disk_write=pre_disk_info[key][9]
                
                end_disk_read=end_disk_info[key][5]
                end_disk_write=end_disk_info[key][9]
                
                disk_read=str((long(end_disk_info[key][5]) - long(pre_disk_info[key][5]))/2)
                #rsect:  (512 bytes/sector) so nee /2                    
                disk_write=str((long(end_disk_info[key][9]) - long(pre_disk_info[key][9]))/2)
                #rsect:  (512 bytes/sector) so nee /2                
                wait_req=end_disk_info[key][11]
                if  self.except_0==1 and  long(disk_read)==0 and  long(disk_write)==0 and  long(wait_req)==0 :                                
                    continue
                else:
                    print(key.ljust(6)+'|'+'disk read'+disk_read.rjust(10)+'kb/s  '\
                      +'disk write'+disk_write.rjust(10)+'kb/s  '+'wait request'\
                      +wait_req.rjust(4)) 
                
                if self.log_to_file==1:
                    fhadle=open(self.log_path+'/disk.log','a')
                    fhadle.writelines(str(self.counter)+','+key.ljust(6)+','+disk_read+','+disk_write+'\r')
                    fhadle.close()
                else:
                    pass                   
    def read_net_usage(self):         
        lines = open("/proc/net/dev").readlines() 
        list_net_info={}
        for line in lines:          
            if 'Inter' in line: 
                continue
            if  'face' in line: 
                continue           
            pos=str(line).index(':')
            key_name=str(line)[:pos]
            key_value=str(line)[pos+1:]            
            net_info = key_value.split() 
            list_net_info[key_name]= net_info
            
        return list_net_info
    
    def net_usage_draw(self,pre_net_info,end_net_info):    
            end_net_info= self.read_net_usage()
            for key in pre_net_info:
               
                pre_net_receive=pre_net_info[key][0]
                end_net_receive=end_net_info[key][0]                
                net_receive=str( round(( long(end_net_receive)-long(pre_net_receive) )/1024))               
                
                pre_net_send=pre_net_info[key][8]
                end_net_send=end_net_info[key][8]                
                net_send=str( round( (long(end_net_send)-long(pre_net_send) )/1024))                 
                if self.except_0==1 and float(net_receive)==0 and float(net_send)==0 :
                    continue
                else :   
                    print(key.ljust(6)+'|'+'net reveive'+net_receive.rjust(10)+'kb/s  '\
                      +'net send'+net_send.rjust(10)+'kb/s  ') 
                
                if self.log_to_file==1:
                    fhadle=open(self.log_path+'/net.log','a')
                    fhadle.writelines(str(self.counter)+','+key.ljust(6)+','+net_receive+','+net_send+'\r')
                    fhadle.close()
                else:
                    pass 
    def read_mem_usage(self):         
        lines = open("/proc/meminfo").readlines() 
        list_mem_info={}
        for line in lines:                   
            pos=str(line).index(':')
            key_name=str(line)[:pos]
            key_value=str(line)[pos+1:]            
            mem_info = key_value.split() 
            list_mem_info[key_name]= mem_info            
        return list_mem_info
    
    def mem_usage_draw(self,mem_info):    
                
        print('MemTotal:'.ljust(10)+ str(round(long(mem_info['MemTotal'][0])/1024)).ljust(15) + ' MB ' + 'MemFree:'.ljust(10) +str(round(long(mem_info['MemFree'][0])/1024)).ljust(15) + ' MB ' )
        print('VmTotal:'.ljust(10)+ str(round(long(mem_info['VmallocTotal'][0])/1024)).ljust(15) + ' MB ' + 'VmUsed:'.ljust(10) +str(round(long(mem_info['VmallocUsed'][0])/1024)).ljust(15) + ' MB ' )
        print('Buffers:'.ljust(10)+ str(round(long(mem_info['Buffers'][0])/1024)).ljust(15) + ' MB ' + 'Cached:'.ljust(10) +str(round(long(mem_info['Cached'][0])/1024)).ljust(15) + ' MB ')                                     
        
        if self.log_to_file==1:
            fhadle=open(self.log_path+'/mem.log','a')
            fhadle.writelines(str(self.counter)+','+'mem'+','+str(round(long(mem_info['MemTotal'][0])/1024))+','+str(round(long(mem_info['MemFree'][0])/1024)) +','\
                              +str(round(long(mem_info['VmallocTotal'][0])/1024))+','+str(round(long(mem_info['VmallocUsed'][0])/1024))+','\
                              +str(round(long(mem_info['Buffers'][0])/1024))+','+str(round(long(mem_info['Cached'][0])/1024))+'\r')
            fhadle.close()
        else:
            pass 
                         
    def usage_draw(self):                    
        if self.log_to_file==1:
            fhadle=open(self.log_path+'/cpu.log','a')
            fhadle.writelines(' id','cpu_id'+','+'cpu_user'+','+'cpu_io'+','+'cpu_sys'+','+'cpu_idel'+'\r')
            fhadle.close()    
        
        if self.log_to_file==1:
            fhadle=open(self.log_path+'/disk.log','a')
            fhadle.writelines(' id','disk_name'+','+'disk_read'+','+'disk_write'+'\r')
            fhadle.close()
        if self.log_to_file==1:
            fhadle=open(self.log_path+'/net.log','a')
            fhadle.writelines(' id','net_name'+','+'net_receive'+','+'net_send'+'\r')
            fhadle.close()
        if self.log_to_file==1:
            fhadle=open(self.log_path+'/mem.log','a')
            fhadle.writelines(' id','mem'+','+'MemTotal'+','+'MemFree' +','\
                              +'VmallocTotal'+','+'VmallocUsed'+','\
                              +'Buffers'+','+'Cached'+'\r')
            fhadle.close()           
        
        
        os.system("clear")
        pre_cpu_info= self.read_cpu_usage()
        pre_disk_info= self.read_disk_usage()
        pre_net_info= self.read_net_usage()
        
        while True: 
            self.counter=self.counter+1               
            time.sleep(self.sleep_time)            
            end_cpu_info= self.read_cpu_usage()
            end_disk_info= self.read_disk_usage()
            end_net_info= self.read_net_usage()
            end_mem_info= self.read_mem_usage()
                        
            os.system("clear")            
            self.cpu_usage_draw(pre_cpu_info, end_cpu_info)
            print "------------------------------------------------------------------------------------------------"
            self.disk_usage_draw(pre_disk_info,end_disk_info)
            print "------------------------------------------------------------------------------------------------"
            self.net_usage_draw(pre_net_info,end_net_info)
            print "------------------------------------------------------------------------------------------------"
            self.mem_usage_draw(end_mem_info)
            
                        
            pre_cpu_info=end_cpu_info 
            pre_disk_info=end_disk_info
            pre_net_info=end_net_info  
                                    
a= hardInfo()
a.sleep_time=1 
a.except_0=1
a.log_to_file=0
a.log_path='/home/bea/why' 
a.usage_draw()





