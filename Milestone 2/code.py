import socket
import time
import threading
import hashlib

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
addr = ('10.17.7.134',9802)

lines = {}
lock_lines = threading.Lock()


def go():
    global send_rate
    global i
    global burst_max
    global stop
    i = 0
    stop = False
    send_rate = 20
    burst_max = 20
    
    while i*1448<= int(res[1]):
        notint = int(burst_max)
        if len(lines)*1448>int(res[1]):
                break
        
        if not stop:
            for j in range(i,i+notint):
                if j*1448 > int(res[1]):
                        break
                if j*1448 not in lines:
                    # print('sending',burst_max,(j)*1448)
                    message = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = j*1448)
                    s.sendto(message.encode('utf-8'), addr)
                    
        stop = True
        time.sleep(1/send_rate)
            
    fill_gaps_thread()



def recv_msg():
    global send_rate
    global i
    global burst_max
    global stop
    s.settimeout(3/send_rate)
    while True:
        try:
            notint = int(burst_max)
            if len(lines)*1448>int(res[1]):
                break
            for t in range(1,notint+1):
                rec = s.recvfrom(2048)
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
                        # print('Received 1448 bytes from offset : ',int(cat[1]),len(lines))
            if ser[2] =='Squished':
                # print('Squished')

                if send_rate//2 >= 0.15:
                    send_rate = send_rate/2
                if burst_max//2 < 1:
                    burst_max = 1
                else:
                    burst_max = burst_max//2
            else:
                if burst_max>=6:
                    
                    burst_max = burst_max+1
                else:
                    send_rate = send_rate+1
                

            i+= notint
            stop = False

        except:
            # print('packet lost i guess')
            if burst_max == 6:
                burst_max = 6
            else:
                burst_max = burst_max-1
            
            send_rate = send_rate+0.5
            i+= notint
            stop = False
            
            

def fill_gaps_thread():
    while len(lines)*1448 < int(res[1]):
        gk = 0
        
        while gk*1448<= int(res[1]):
            if (gk*1448) not in lines:
                try:
                    message1 = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = gk*1448)
                    act1 = message1.encode('utf-8')
                    s.sendto(act1, addr)
                    # print('filling gaps for offset: ',gk*1448)
                    time.sleep(0.01)
                    
                except:
                    yth = 45
                    # print('finishing')
            gk+= 1
    final_submit()



def final_submit():
    
    sorted_lines = [lines[key] for key in sorted(lines.keys())]   
    conc = ''.join(sorted_lines)
    conc = conc[:-1]
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


if __name__ == "__main__":
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
    