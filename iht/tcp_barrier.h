#pragma once

#include <remus/logging/logging.h>
#include <remus/util/tcp/tcp.h>

using namespace remus::util;

namespace ExperimentManager {
/// A barrier to arrive at. Joined with ServerStopBarrier
inline void ClientArriveBarrier(tcp::EndpointManager* socket_handle){
  // Receive a message from all clients to sync
  tcp::message msg;
  socket_handle->send_server(&msg);
  REMUS_DEBUG("CLIENT :: Sent ack");
  socket_handle->recv_server(&msg);
  REMUS_DEBUG("CLIENT :: Recevied ack");
}

/// @brief Sleep (to avoid taking up resources) and then try to sync a exit with the clients
/// @param socket_handle the socket manager resource for communicating with remote clients
/// @param runtime_s how long to wait before listening for finishing messages
/// @param cleanup a cleanup script to run every 100ms 
/// [esl] IMP: cleanup was removed because it is used in the other hashmap but not the IHT
///            Thus it is not necessary for a minimal IHT. I left it in the documentation because I'd like 
///            to revisit implementing a cleanup function to allow for things such as remote deallocation.
/// @return ok status
inline void ServerStopBarrier(tcp::SocketManager* socket_handle, int runtime_s) {
  // Sleep while clients are running if there is a set runtime.
  if (runtime_s > 0) {
    REMUS_INFO("SERVER :: Sleeping for {}", runtime_s);

    // Sleep for runtime seconds while the clients are running
    std::this_thread::sleep_for(std::chrono::seconds(runtime_s));
  }

  // [esl] IMP: The purpose of the tcp module is not to be efficient, but rather to be able to serve as a simple barrier 
  // at least until an efficient RDMA-based one can be created
  // It also serves the function of sending the remote_ptr, which is why the API is not a barrier but more of a server-client (one->many relationship)

  // Receive a message from all clients to sync
  tcp::message recv_buffer[socket_handle->num_clients()];
  socket_handle->recv_from_all(recv_buffer);
  REMUS_DEBUG("SERVER :: received ack");
  
  // Once we receive a message from everyone, everyone is done
  // So we now send an OK to exit message
  tcp::message send_buffer;
  socket_handle->send_to_all(&send_buffer);
  REMUS_DEBUG("SERVER :: sent ack");
}
};
