import socket
import time
import threading
import hashlib
import math
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
addr = ('10.17.51.115',9802)

lines = {}
lock_lines = threading.Lock()


def go():
    global send_rate
    global i
    global all_not_good
    global increment
    global burst
    global wait
    
    wait = True
    all_not_good = True
    # stop = True
    send_rate = 35
    increment = 1
    burst = 5
    
    #print('now here')

    for k in range(0,burst):
        if k*1448 not in lines:
            message = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = k*1448)
            print('sending1',k*1448,increment)
            s.sendto(message.encode('utf-8'), addr)
    

    i = burst
    # print('0')
    # if not stop:
    
    while i*1448<= int(res[1]):
        # print('1')
        # print(wait)
        if not wait:
            #print('4')  
            if i*1448 not in lines:
                message = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = i*1448)
                s.sendto(message.encode('utf-8'), addr)
                print('sending3',send_rate,i*1448,increment)
                time.sleep(1/send_rate)
        wait = True


        if not all_not_good:
            #print('2')
            for t in range(i,i+increment+1):
                if t*1448 not in lines:
                    message = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = t*1448)
                    print('sending2',t*1448,increment)
                    s.sendto(message.encode('utf-8'), addr)
                    time.sleep(1/send_rate)
            if increment==10:
                increment = 10
            else:
                increment += 1      
            all_not_good = True
            
            
        
    fill_gaps_thread()




def recv_msg():
    global send_rate
    global i
    global burst
    global increment
    global all_not_good
    global wait
    prev_burst = 1
    s.settimeout(1/send_rate)
    burst = 5
    while True:
        try:
            if len(lines)*1448>int(res[1]):
                break
            #print('waiting')
            rec = s.recvfrom(2048)
            #print('received')
            zexi = rec[0].decode('utf-8')
            ser = zexi.split('\n')
            cat = []
            for item in ser:
                items = item.split(':')
                cat.extend(items)
            if int(cat[1]) not in lines:
                loc = zexi.index('\n\n')
                mk = zexi[loc+2:]
                lines[int(cat[1])] = mk
                print('Received 1448 bytes from offset : ',int(cat[1]),len(lines))
                if ser[2] =='Squished':
                    print('Squished')
                    send_rate = 1
                    burst = 1
                    increment = 1
                    wait = False
                    i+= 1
                else:
                    # print('Not Squished')
                    send_rate+= 1
                    burst+= 1/burst
                    print('burst',burst,'prev_burst',prev_burst)
                    
                    if math.floor(burst)==prev_burst+1:
                        all_not_good = False
                      
                    elif math.floor(burst) == prev_burst:
                        print('burst',burst)
                        wait = False
                    prev_burst = int(burst)  
                    i+= 1
                    
                        
        except:
            i+= 1
            wait = False
            

            

def fill_gaps_thread():
    while len(lines)*1448 < int(res[1]):
        gk = 0
        
        while gk*1448<= int(res[1]):
            if (gk*1448) not in lines:
                try:
                    message1 = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = gk*1448)
                    act1 = message1.encode('utf-8')
                    s.sendto(act1, addr)
                    print('filling gaps for offset: ',gk*1448)
                    time.sleep(0.01)
                    
                except:
                    yth = 45
                    print('finishing')
            gk+= 1
    final_submit()



def final_submit():
    
    sorted_lines = [lines[key] for key in sorted(lines.keys())]   
    conc = ''.join(sorted_lines)
    conc = remove_last_line(conc)
    conc += '\n'
    md5_hash = hashlib.md5(conc.encode()).hexdigest()
    

    max_retries = 3  # Maximum number of retries

    for retry in range(max_retries):
        try:
            mess = 'Submit: 2021CS10109\nMD5: {md5_hash1}\n\n'.format(md5_hash1=md5_hash)
            qm = mess.encode('utf-8')
            s.sendto(qm, addr)
            nano = s.recvfrom(2048)
            zex = nano[0].decode('utf-8')
            print('--------------')
            print(zex)
            s.close()
            print('socket closed')
            break  # Exit the loop if the operation succeeds
        except Exception as e:
            # print('Exception:', str(e))
            if retry < max_retries - 1:
                print('Submit response not received\nRetrying...')
                continue  # Retry the operation
            
        print('Max retries reached. Operation failed.')


def remove_last_line(text):
    lines = text.split('\n')
    if len(lines) > 0:
        lines.pop()
    modified_text = '\n'.join(lines)
    
    return modified_text




send_handler = threading.Thread(target=go)
rec_handler = threading.Thread(target=recv_msg)



try:
    start = time.time()
    message = 'SendSize\nReset\n\n'
    act = message.encode('utf-8')
    s.sendto(act, addr)
    recvievet = s.recvfrom(2048)
    size = recvievet[0].decode('utf-8')
    real = size.split('\n')
    res= []
    for item in real:
        items = item.split(':')
        res.extend(items)

    print('res[1]',res[1])
    send_handler.start()
    rec_handler.start()
    

    send_handler.join()
    rec_handler.join()
    

except:
    print('fbhfdsnrehfjkd')