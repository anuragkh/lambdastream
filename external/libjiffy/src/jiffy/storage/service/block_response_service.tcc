/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "block_response_service.h"
#ifndef block_response_service_TCC
#define block_response_service_TCC


namespace jiffy { namespace storage {


template <class Protocol_>
uint32_t block_response_service_response_args::read(Protocol_* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->seq.read(iprot);
          this->__isset.seq = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->result.clear();
            uint32_t _size52;
            ::apache::thrift::protocol::TType _etype55;
            xfer += iprot->readListBegin(_etype55, _size52);
            this->result.resize(_size52);
            uint32_t _i56;
            for (_i56 = 0; _i56 < _size52; ++_i56)
            {
              xfer += iprot->readBinary(this->result[_i56]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.result = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

template <class Protocol_>
uint32_t block_response_service_response_args::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_response_args");

  xfer += oprot->writeFieldBegin("seq", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->seq.write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("result", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->result.size()));
    std::vector<std::string> ::const_iterator _iter57;
    for (_iter57 = this->result.begin(); _iter57 != this->result.end(); ++_iter57)
    {
      xfer += oprot->writeBinary((*_iter57));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_response_pargs::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_response_pargs");

  xfer += oprot->writeFieldBegin("seq", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += (*(this->seq)).write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("result", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>((*(this->result)).size()));
    std::vector<std::string> ::const_iterator _iter58;
    for (_iter58 = (*(this->result)).begin(); _iter58 != (*(this->result)).end(); ++_iter58)
    {
      xfer += oprot->writeBinary((*_iter58));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_chain_ack_args::read(Protocol_* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRUCT) {
          xfer += this->seq.read(iprot);
          this->__isset.seq = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

template <class Protocol_>
uint32_t block_response_service_chain_ack_args::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_chain_ack_args");

  xfer += oprot->writeFieldBegin("seq", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += this->seq.write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_chain_ack_pargs::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_chain_ack_pargs");

  xfer += oprot->writeFieldBegin("seq", ::apache::thrift::protocol::T_STRUCT, 1);
  xfer += (*(this->seq)).write(oprot);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_notification_args::read(Protocol_* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->op);
          this->__isset.op = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readBinary(this->data);
          this->__isset.data = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

template <class Protocol_>
uint32_t block_response_service_notification_args::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_notification_args");

  xfer += oprot->writeFieldBegin("op", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString(this->op);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("data", ::apache::thrift::protocol::T_STRING, 2);
  xfer += oprot->writeBinary(this->data);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_notification_pargs::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_notification_pargs");

  xfer += oprot->writeFieldBegin("op", ::apache::thrift::protocol::T_STRING, 1);
  xfer += oprot->writeString((*(this->op)));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("data", ::apache::thrift::protocol::T_STRING, 2);
  xfer += oprot->writeBinary((*(this->data)));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_control_args::read(Protocol_* iprot) {

  ::apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast59;
          xfer += iprot->readI32(ecast59);
          this->type = (response_type)ecast59;
          this->__isset.type = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->ops.clear();
            uint32_t _size60;
            ::apache::thrift::protocol::TType _etype63;
            xfer += iprot->readListBegin(_etype63, _size60);
            this->ops.resize(_size60);
            uint32_t _i64;
            for (_i64 = 0; _i64 < _size60; ++_i64)
            {
              xfer += iprot->readString(this->ops[_i64]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.ops = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->error);
          this->__isset.error = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

template <class Protocol_>
uint32_t block_response_service_control_args::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_control_args");

  xfer += oprot->writeFieldBegin("type", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32((int32_t)this->type);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("ops", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->ops.size()));
    std::vector<std::string> ::const_iterator _iter65;
    for (_iter65 = this->ops.begin(); _iter65 != this->ops.end(); ++_iter65)
    {
      xfer += oprot->writeString((*_iter65));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("error", ::apache::thrift::protocol::T_STRING, 3);
  xfer += oprot->writeString(this->error);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}


template <class Protocol_>
uint32_t block_response_service_control_pargs::write(Protocol_* oprot) const {
  uint32_t xfer = 0;
  ::apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
  xfer += oprot->writeStructBegin("block_response_service_control_pargs");

  xfer += oprot->writeFieldBegin("type", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32((int32_t)(*(this->type)));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("ops", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>((*(this->ops)).size()));
    std::vector<std::string> ::const_iterator _iter66;
    for (_iter66 = (*(this->ops)).begin(); _iter66 != (*(this->ops)).end(); ++_iter66)
    {
      xfer += oprot->writeString((*_iter66));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("error", ::apache::thrift::protocol::T_STRING, 3);
  xfer += oprot->writeString((*(this->error)));
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::response(const sequence_id& seq, const std::vector<std::string> & result)
{
  send_response(seq, result);
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::send_response(const sequence_id& seq, const std::vector<std::string> & result)
{
  int32_t cseqid = 0;
  this->oprot_->writeMessageBegin("response", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_response_pargs args;
  args.seq = &seq;
  args.result = &result;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::chain_ack(const sequence_id& seq)
{
  send_chain_ack(seq);
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::send_chain_ack(const sequence_id& seq)
{
  int32_t cseqid = 0;
  this->oprot_->writeMessageBegin("chain_ack", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_chain_ack_pargs args;
  args.seq = &seq;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::notification(const std::string& op, const std::string& data)
{
  send_notification(op, data);
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::send_notification(const std::string& op, const std::string& data)
{
  int32_t cseqid = 0;
  this->oprot_->writeMessageBegin("notification", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_notification_pargs args;
  args.op = &op;
  args.data = &data;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::control(const response_type type, const std::vector<std::string> & ops, const std::string& error)
{
  send_control(type, ops, error);
}

template <class Protocol_>
void block_response_serviceClientT<Protocol_>::send_control(const response_type type, const std::vector<std::string> & ops, const std::string& error)
{
  int32_t cseqid = 0;
  this->oprot_->writeMessageBegin("control", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_control_pargs args;
  args.type = &type;
  args.ops = &ops;
  args.error = &error;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();
}

template <class Protocol_>
bool block_response_serviceProcessorT<Protocol_>::dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext) {
  typename ProcessMap::iterator pfn;
  pfn = processMap_.find(fname);
  if (pfn == processMap_.end()) {
    iprot->skip(::apache::thrift::protocol::T_STRUCT);
    iprot->readMessageEnd();
    iprot->getTransport()->readEnd();
    ::apache::thrift::TApplicationException x(::apache::thrift::TApplicationException::UNKNOWN_METHOD, "Invalid method name: '"+fname+"'");
    oprot->writeMessageBegin(fname, ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return true;
  }
  (this->*(pfn->second.generic))(seqid, iprot, oprot, callContext);
  return true;
}

template <class Protocol_>
bool block_response_serviceProcessorT<Protocol_>::dispatchCallTemplated(Protocol_* iprot, Protocol_* oprot, const std::string& fname, int32_t seqid, void* callContext) {
  typename ProcessMap::iterator pfn;
  pfn = processMap_.find(fname);
  if (pfn == processMap_.end()) {
    iprot->skip(::apache::thrift::protocol::T_STRUCT);
    iprot->readMessageEnd();
    iprot->getTransport()->readEnd();
    ::apache::thrift::TApplicationException x(::apache::thrift::TApplicationException::UNKNOWN_METHOD, "Invalid method name: '"+fname+"'");
    oprot->writeMessageBegin(fname, ::apache::thrift::protocol::T_EXCEPTION, seqid);
    x.write(oprot);
    oprot->writeMessageEnd();
    oprot->getTransport()->writeEnd();
    oprot->getTransport()->flush();
    return true;
  }
  (this->*(pfn->second.specialized))(seqid, iprot, oprot, callContext);
  return true;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_response(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext)
{
  (void) seqid;
  (void) oprot;
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.response", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.response");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.response");
  }

  block_response_service_response_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.response", bytes);
  }

  try {
    iface_->response(args.seq, args.result);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.response");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.response");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_response(int32_t, Protocol_* iprot, Protocol_*, void* callContext)
{
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.response", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.response");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.response");
  }

  block_response_service_response_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.response", bytes);
  }

  try {
    iface_->response(args.seq, args.result);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.response");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.response");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_chain_ack(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext)
{
  (void) seqid;
  (void) oprot;
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.chain_ack", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.chain_ack");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.chain_ack");
  }

  block_response_service_chain_ack_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.chain_ack", bytes);
  }

  try {
    iface_->chain_ack(args.seq);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.chain_ack");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.chain_ack");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_chain_ack(int32_t, Protocol_* iprot, Protocol_*, void* callContext)
{
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.chain_ack", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.chain_ack");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.chain_ack");
  }

  block_response_service_chain_ack_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.chain_ack", bytes);
  }

  try {
    iface_->chain_ack(args.seq);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.chain_ack");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.chain_ack");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_notification(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext)
{
  (void) seqid;
  (void) oprot;
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.notification", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.notification");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.notification");
  }

  block_response_service_notification_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.notification", bytes);
  }

  try {
    iface_->notification(args.op, args.data);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.notification");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.notification");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_notification(int32_t, Protocol_* iprot, Protocol_*, void* callContext)
{
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.notification", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.notification");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.notification");
  }

  block_response_service_notification_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.notification", bytes);
  }

  try {
    iface_->notification(args.op, args.data);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.notification");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.notification");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_control(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext)
{
  (void) seqid;
  (void) oprot;
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.control", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.control");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.control");
  }

  block_response_service_control_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.control", bytes);
  }

  try {
    iface_->control(args.type, args.ops, args.error);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.control");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.control");
  }

  return;
}

template <class Protocol_>
void block_response_serviceProcessorT<Protocol_>::process_control(int32_t, Protocol_* iprot, Protocol_*, void* callContext)
{
  void* ctx = NULL;
  if (this->eventHandler_.get() != NULL) {
    ctx = this->eventHandler_->getContext("block_response_service.control", callContext);
  }
  ::apache::thrift::TProcessorContextFreer freer(this->eventHandler_.get(), ctx, "block_response_service.control");

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->preRead(ctx, "block_response_service.control");
  }

  block_response_service_control_args args;
  args.read(iprot);
  iprot->readMessageEnd();
  uint32_t bytes = iprot->getTransport()->readEnd();

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->postRead(ctx, "block_response_service.control", bytes);
  }

  try {
    iface_->control(args.type, args.ops, args.error);
  } catch (const std::exception&) {
    if (this->eventHandler_.get() != NULL) {
      this->eventHandler_->handlerError(ctx, "block_response_service.control");
    }
    return;
  }

  if (this->eventHandler_.get() != NULL) {
    this->eventHandler_->asyncComplete(ctx, "block_response_service.control");
  }

  return;
}

template <class Protocol_>
::apache::thrift::stdcxx::shared_ptr< ::apache::thrift::TProcessor > block_response_serviceProcessorFactoryT<Protocol_>::getProcessor(const ::apache::thrift::TConnectionInfo& connInfo) {
  ::apache::thrift::ReleaseHandler< block_response_serviceIfFactory > cleanup(handlerFactory_);
  ::apache::thrift::stdcxx::shared_ptr< block_response_serviceIf > handler(handlerFactory_->getHandler(connInfo), cleanup);
  ::apache::thrift::stdcxx::shared_ptr< ::apache::thrift::TProcessor > processor(new block_response_serviceProcessorT<Protocol_>(handler));
  return processor;
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::response(const sequence_id& seq, const std::vector<std::string> & result)
{
  send_response(seq, result);
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::send_response(const sequence_id& seq, const std::vector<std::string> & result)
{
  int32_t cseqid = 0;
  ::apache::thrift::async::TConcurrentSendSentry sentry(&this->sync_);
  this->oprot_->writeMessageBegin("response", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_response_pargs args;
  args.seq = &seq;
  args.result = &result;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();

  sentry.commit();
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::chain_ack(const sequence_id& seq)
{
  send_chain_ack(seq);
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::send_chain_ack(const sequence_id& seq)
{
  int32_t cseqid = 0;
  ::apache::thrift::async::TConcurrentSendSentry sentry(&this->sync_);
  this->oprot_->writeMessageBegin("chain_ack", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_chain_ack_pargs args;
  args.seq = &seq;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();

  sentry.commit();
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::notification(const std::string& op, const std::string& data)
{
  send_notification(op, data);
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::send_notification(const std::string& op, const std::string& data)
{
  int32_t cseqid = 0;
  ::apache::thrift::async::TConcurrentSendSentry sentry(&this->sync_);
  this->oprot_->writeMessageBegin("notification", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_notification_pargs args;
  args.op = &op;
  args.data = &data;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();

  sentry.commit();
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::control(const response_type type, const std::vector<std::string> & ops, const std::string& error)
{
  send_control(type, ops, error);
}

template <class Protocol_>
void block_response_serviceConcurrentClientT<Protocol_>::send_control(const response_type type, const std::vector<std::string> & ops, const std::string& error)
{
  int32_t cseqid = 0;
  ::apache::thrift::async::TConcurrentSendSentry sentry(&this->sync_);
  this->oprot_->writeMessageBegin("control", ::apache::thrift::protocol::T_ONEWAY, cseqid);

  block_response_service_control_pargs args;
  args.type = &type;
  args.ops = &ops;
  args.error = &error;
  args.write(this->oprot_);

  this->oprot_->writeMessageEnd();
  this->oprot_->getTransport()->writeEnd();
  this->oprot_->getTransport()->flush();

  sentry.commit();
}

}} // namespace

#endif
