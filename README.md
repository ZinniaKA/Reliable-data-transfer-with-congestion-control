# Reliable-data-transfer-with-congestion-control
Assignment 3 of the course Computer Networks at IIT Delhi Sem 1 23-24

Implement a TCP-like protocol for reliable data transfer, with congestion control-like mechanisms to not overwhelm the network yet obtain high throughput.Your job is to implement the client. The client sends requests to the server to receive a certain number of bytes from a certain offset. The server replies to the client with this data, but it makes several checks to decide whether to reply or not:

- To emulate a lossy network, the server randomly decides to not reply every now and then. Internally it samples from a uniformly random distribution and provides a constant packet loss rate.
- To force clients to not send requests too fast, the server implements a leaky bucket filter for each client and replies only if a client’s bucket has remaining tokens. A leaky bucket operates on two parameters: a rate and a bucket-size. Tokens are generated at a constant rate, but only a maximum of bucket-size number of tokens can be retained. Whenever the server replies, a token is consumed. If a client makes requests too fast and empties out the bucket (i.e. all tokens have been consumed) then the server does not reply to a new request. After some time, new tokens would get generated (based on token generation rate), and now new client requests would be serviced by the server. Leaky buckets are used extensively in the Internet to shape bursty traffic.
- The server further keeps track of requests that could not be serviced because enough tokens werenot available. To condone rude behavior when clients keep sending requests even though the serveris not replying to them, as a penalty the server squishes them by temporarily reducing the rate of generating new tokens, i.e. it would now take longer for clients to receive data.

You should do this assignment step by step. We will also have intermediate milestones that you should meet by the indicated deadline.

## Milestone 1: Due Oct 15th

- Start with demonstrating that you can send and receive UDP packets in accordance with the protocol specified here.
- Next, show that you can receive data reliably.Verify the MD5 hash from the server.
  
## Milestone 2: Due Oct 22nd

- Now, implement your client for the case where the server maintains a constant rate for its leaky bucket parameters.
  
## Milestone 3: Due Oct 29th

- Finally, generalize your client to be able to handle variable rate scenarios. Report the timing, explain
your methods, and show graphs that can help understand this better.

The folder contains code and report for each milestones.
