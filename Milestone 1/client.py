import socket
import time
import threading
import hashlib

#UDP Socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
addr = ("127.0.0.1", 9801)
s.settimeout(2)
lines = {}
lock_lines = threading.Lock()

def send_thread():
    i = 0
    j = 1448
    print(f'size expected to receive : {res[1]}')
    for i in range(0,int(res[1])+1):
            message1 = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = i)
            act1 = message1.encode('utf-8')
            s.sendto(act1, addr)
            print('Requested 1448 bytes from offset : ',i)
            i = i + 1448
            time.sleep(0.01)
            
def receive_thread():
    while True:
        try:
            rec= s.recvfrom(2048)
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
                print('Received 1448 bytes from offset : ',int(cat[1]))
        except:
            h = 8

def fill_gaps_thread():
    while len(lines)*1448 < int(res[1]):
        gk = 0
        #print('[]]]]]]]]]]]]')
        while gk*1448<= int(res[1]):
            if (gk*1448) not in lines:
                try:
                    message1 = 'Offset: {offset}\nNumBytes: 1448\n\n'.format(offset = gk*1448)
                    act1 = message1.encode('utf-8')
                    s.sendto(act1, addr)
                    print('Request resent for offset : ',gk*1448)
                    time.sleep(0.01)
                    
                except:
                    yth = 45
            gk+= 1
            
    
    final_submit()


def remove_last_line(text):
    lines = text.split('\n')
    if len(lines) > 0:
        lines.pop()
    modified_text = '\n'.join(lines)
    
    return modified_text

def final_submit():
    # print(len(lines))
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


send_handler = threading.Thread(target=send_thread)
receive_handler = threading.Thread(target=receive_thread)
fill_gaps_handler = threading.Thread(target=fill_gaps_thread)

try:
    message = 'SendSize\n\n'
    act = message.encode('utf-8')
    s.sendto(act, addr)
    recvievet = s.recvfrom(2048)
    size = recvievet[0].decode('utf-8')
    real = size.split('\n')
    res= []
    for item in real:
        items = item.split(':')
        res.extend(items)
    send_handler.start()
    receive_handler.start()
    fill_gaps_handler.start()

    send_handler.join()
    receive_handler.join()
    fill_gaps_handler.join()


finally:
    s.close()
    print('socket closed1')





