// ===========================================================================
// Copyright (c) 2017 by
// ZQ Interactive, Inc., Shanghai, PRC.,
// All Rights Reserved.  Unpublished rights reserved under the copyright
// laws of the United States.
// 
// The software contained  on  this media is proprietary to and embodies the
// confidential technology of ZQ Interactive, Inc. Possession, use,
// duplication or dissemination of the software and media is authorized only
// pursuant to a valid written license from ZQ Interactive, Inc.
// 
// This software is furnished under a  license  and  may  be used and copied
// only in accordance with the terms of  such license and with the inclusion
// of the above copyright notice.  This software or any other copies thereof
// may not be provided or otherwise made available to  any other person.  No
// title to and ownership of the software is hereby transferred.
//
// The information in this software is subject to change without notice and
// should not be construed as a commitment by ZQ Interactive, Inc.
//
// Ident : $Id: SharedPage.h $
// Branch: $Name:  $
// Author: Hui Shao
// Desc  : Define stream segment
//
// Revision History: 
// ---------------------------------------------------------------------------
// $Log: /ZQProjs/Common/Exception.h $
// ===========================================================================

#ifndef __streamcraft_LargeContentPage_H__
#define __streamcraft_LargeContentPage_H__

#include "ZQ_common_conf.h"
#include "Pointer.h"
#include "Log.h"
#include "Locks.h"
#include "eloop.h"
#include "LRUMap.h"

#include <list>
namespace syscheme {
namespace StreamCraft {

#define INVALID_FD           (-1)
#define SEGMENT_BITLEN       (21) // 2MB
#define SEGMENT_SIZE         (1<<(SEGMENT_BITLEN)) // 2MB
#define SEGMENT_KEYLEN       11   // do brace this value, 11hex=44bit, (44+21) is enough to hold a 64bit filesize in SEGMENT_SIZE
#ifndef SEG_IDX_OF_OFFSET
#define SEG_IDX_OF_OFFSET(_OFFSET)  ((_OFFSET) >> SEGMENT_BITLEN)
#endif
#define SEG_STARTOFFSET(_SEGIDX)    (((int64)_SEGIDX) <<SEGMENT_BITLEN)
#define ROUND_DOWN_OFFSET(_OFFSET)  (SEG_STARTOFFSET(SEG_IDX_OF_OFFSET(_OFFSET)))

#ifndef FLAG
#  define FLAG(_BIT) (1 << _BIT)
#endif // FLAG

#ifndef TSPACKET_LENTH
#  define TSPACKET_LENTH   (188)
#endif // TSPACKET_LENTH

class CPMISERVANT(PageAllocator);

// -----------------------------
// class SharedPage
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
class SharedPage : public ZQ::common::SharedObject
{
	friend class CPMISERVANT(PageAllocator);
	friend class PageFile;

public:
	typedef ZQ::common::Pointer< SharedPage > Ptr;
	typedef std::list<Ptr> Segments;
//	typedef std::queue< Ptr > Queue;

	typedef struct _PageDescriptor
	{
		std::string key;   // the unique key to identify the buf
		int    fd;         // the fd to the shm
		uint32 capacity;    // the maximal bytes of pay-load data that is allowed to filled into the buffer, excluding the size of file header/footer if have any
		uint8* ptr;        // the memory address if valid
		uint32 length;     // the bytes of avaliable data start from offset=0 in the fd
		uint32 flags;      // the fd to the shm
		int64  stampAsOfOrigin;   // the timestamp as of when received from the orign

		std::string cpmi_allocator; // the url to the allocator who owns the page
		_PageDescriptor() { fd = INVALID_FD; capacity = 0; ptr = NULL; length = 0; flags = 0; }

	} PageDescriptor;

	typedef enum _StreamSegmentFlags
	{
		sfReadOnly,
		sfDirtyLeading,
	} StreamSegmentFlags;

protected:

	CPMISERVANT(PageAllocator)& _page_mgr;
	PageDescriptor  _bufd;
	int64          _offsetOfOrigin; // the offset of the source file that offset = 0 in the fd maps to
	int            _headLength;

	// only hatched by the SegementManager
	SharedPage(PageManager& sm, PageDescriptor& bufd, const std::string& uriOrigin, int64 offsetOrigin, int HeadLength = 0);

public:

	virtual ~SharedPage();

	int fd() const { return _bufd.fd; }
	uint flags() const { return _bufd.flags; }

	bool isReadOnly() const { return FLAG_ISSETn(_bufd.flags, sfReadOnly); }
	void setReadOnly(bool readonly=true);
	bool isDirtyLeading() const { return FLAG_ISSETn(_bufd.flags, sfDirtyLeading); }

	void setFlag(StreamSegmentFlags flag) { FLAG_SETn(_bufd.flags, flag);}
	
	std::string key() const { return _bufd.key;  }
	int64 getOffsetOfOrigin() const { return _offsetOfOrigin;  }
	void  setOffsetOfOrigin(int64 offset)  { _offsetOfOrigin = offset; }
	int64 getstampAsOfOrigin() const { return _bufd.stampAsOfOrigin;  }
	int getCapacity() const { return _bufd.capacity; }
	int getLength() {return _bufd.length;}
	uint addLength(uint len);

	virtual uint8* ptr();
	void resetAttr(const std::string& uriOrigin, int64 offsetOrigin);

	// fill data from the source to this segment by fd
	//@param fdSource, the fd of the source
	//@param length, the number of bytes to read from the source and fill into the segment
	//@param startOffset, if >=0, to specify the offset within the segment where to start fill
	//@return the number of bytes has successfully filled into the segment
	virtual int fill(int fdSource, uint length, int startOffset = -1);
	
	// fill data from the source to this segment by memory address
	//@param source, the memory address of the source
	//@param length, the number of bytes to read from the source and fill into the segment
	//@param startOffset, if >=0, to specify the offset within the segment where to start fill
	//@return the number of bytes has successfully filled into the segment
	virtual int fill(uint8* source, uint length, int startOffset = -1);

	// output data from the segement to a target by fd
	//@param fdTarget, the fd of the target
	//@param length, the number of bytes to read from the segment
	//@param startOffset, if >=0, to specify the offset within the segment where to start flushing
	//@return the number of bytes has successfully filled into the segment
	virtual int flush(int fdTarget, uint length, int startOffset = -1);

	// output data from the segement to a target by memory address
	//@param target, the memory address of the target
	//@param length, the number of bytes to read from the source and fill into the segment
	//@param startOffset, if >=0, to specify the offset within the segment where to start fill
	//@return the number of bytes has successfully filled into the segment
	virtual int flush(uint8* target, uint length, int startOffset = -1);
};

// -----------------------------
// interface PageAllocator
// -----------------------------
class PageAllocator
{
public:
	static bool junmarshal_PageDescriptor(SharedPage::PageDescriptor& pd, const Json::Value& value);
	static void jmarshal_PageDescriptor(const SharedPage::PageDescriptor& pd, Json::Value& value);

	DECLARE_CPMI_API(allocate,  (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished), (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished, const std::string& reqtxn));
	DECLARE_CPMI_API(free,      (SharedPage::PageDescriptor& pd),                                                                  (SharedPage::PageDescriptor& pd, const std::string& reqtxn));
};

DECLARE_STUB_START(PageAllocator)
	DECALRE_CPMICLIENT_API(allocate,  (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished), (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished, const std::string& reqtxn));
	DECALRE_CPMICLIENT_API(free,      (SharedPage::PageDescriptor& pd),                                                                  (SharedPage::PageDescriptor& pd, const std::string& reqtxn));

protected:                                                                  
	virtual void OnAllocted(PageDescriptor& pd, const char* by=NULL) {}
DECLARE_STUB_END(PageAllocator);

DECLARE_SERVANT_START(PageAllocator)
	DECALRE_CPMISERVER_API(allocate,  (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished), (SharedPage::PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished, const std::string& reqtxn));
	DECALRE_CPMISERVER_API((free,     (SharedPage::PageDescriptor& pd),                                                                  (SharedPage::PageDescriptor& pd, const std::string& reqtxn));
DECLARE_SERVANT_END(PageAllocator);

// -----------------------------
// class SharedPageRelay as a proxy to forward alloc/free to a remote
// -----------------------------
class SharedPageRelay : public CPMIAPP(PageAllocator)
{
	friend class SharedPage;
public:
	SharedPageRelay(ZQ::common::Log& log, ZQ::sloop::InterruptibleLoop& sloop, ZQ::sloop::CPMIService& cpmi);
	virtual ~SharedPageRelay() {}

	uint16	getVerbosity() { return (ZQ::common::Log::loglevel_t) _lw.getVerbosity() | (_verboseFlags<<8); }
	void    setVerbosity(uint16 verbose = (0 | ZQ::common::Log::L_ERROR)) { _lw.setVerbosity(verbose & 0x0f); _verboseFlags =verbose>>8; }

	// allocate a segment with key as the purpsoe
	// the function is designed to support retries
	///@return pointer to the allocated segment if successful; NULL if fail
	///         - NULL when the PageManager is busy or waiting for file load of DiskCache, the caller can retry after an interval
	///         - NULL if out-of-memory after failure/timeout at DiskCache loading
	virtual SharedPage::Ptr allocate(const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished=false) { return NULL; }

	// free an allocated page
	virtual void free(SharedPage::PageDescriptor& bufd)
	{
		CPMICLIENT(APICLS)::Ptr client;
		if (!bufd.cpmi_allocator.empty()) 
			client= open_remote(bufd.bufd.cpmi_allocator);
		if (!client) return;

		client->call_free(bufd.bufd);
	}

protected:
	virtual CPMICLIENT(APICLS)::Ptr open_remote(const std::string& cpmi_allocator); // may lookup _prxMap first

	ZQ::common::LogWrapper	_lw;
	uint16					_verboseFlags;
	std::map <std::string, CPMICLIENT(PageAllocator)::Ptr> _prxMap; // should be LRUMap
};

// -----------------------------
// class PageManager that extends SharedPageRelay with local page pool
// -----------------------------
class PageManager : public SharedPageRelay // inherit SharedPageRelay as a forwarder in the case PageDescriptor.cpmi_allocator refer to remote
{
	friend class SharedPage;

public:
	typedef std::list<SharedPage::PageDescriptor> SegmentList;

public:
	PageManager(ZQ::common::Log& log, ZQ::sloop::InterruptibleLoop& sloop, ZQ::sloop::CPMIService& cpmi, uint segPoolInit =0, uint segPoolMax = 4000, const ZQ::sloop::CPMIHandler::Properties& appProps);
	virtual ~PageManager() { clear(); }

public: // overwrite SharedPageRelay
	// allocate a segment with key as the purpsoe
	// the function is designed to support retries
	///@return pointer to the allocated segment if successful; NULL if fail
	///         - NULL when the PageManager is busy or waiting for file load of DiskCache, the caller can retry after an interval
	///         - NULL if out-of-memory after failure/timeout at DiskCache loading
	virtual SharedPage::Ptr allocate(const std::string& uriOrigin, int64 offsetOrigin);
	virtual void free(SharedPage::PageDescriptor& bufd);

public: // overwrite SharedPageRelay
	// SharedPage::Ptr allocateEmptyPacket(const std::string& uriOrigin, int64 offsetOrigin);
	// void freeEmptyPacket(SharedPage::Ptr seg);
	SharedPage::Ptr newNullSegment(const std::string& uriOrigin, int64 offsetOrigin);
	void freeNullSegment(SharedPage::Ptr seg);

	virtual size_t freeSize() { return _segIdle.size(); }

	virtual void onFull(SharedPage::Ptr seg);
	virtual void updateSegment(SharedPage::Ptr seg);

	void clear();

	//	ZQ::common::Log& _log;
protected:	
	virtual void free(SharedPage::Ptr seg);

	template <class SegmentT>
	SharedPage::Ptr hatchSegObj(SharedPage::PageDescriptor& bufd, const std::string& uriOrigin = "default", int64 offsetOrigin = 0)
	{
		if(bufd.key.empty())
			bufd.key = PageManager::genShmkey(uriOrigin, offsetOrigin);

		SharedPage::Ptr seg = new SegmentT(*this, bufd, uriOrigin, offsetOrigin);
		return seg;
	}

public:
	static  std::string genShmkey(const std::string& uriOrigin, int64 offsetOrigin);

private:
	SegmentList				_segPool;
	SegmentList				_segIdle;
	uint					_segPoolMax;

public:	// impl of CPMIAPP(PageAllocator)
	// virtual ZQ::sloop::CPMIMessage::Error impl_queryPD(const std::string& clipName, PortStatus::PDInfo& result, const std::string& reqtxn);
	virtual ZQ::sloop::CPMIMessage::Error impl_allocate(PageDescriptor& pd, const std::string& uriOrigin, int64 offsetOrigin, bool prefill_wished, const std::string& reqtxn);
	virtual ZQ::sloop::CPMIMessage::Error impl_free(PageDescriptor& pd, const std::string& reqtxn);
};

}}//namespace

#endif // __streamcraft_LargeContentPage_H__
