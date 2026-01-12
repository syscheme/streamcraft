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
// Ident : $Id: StreamWin.h $
// Branch: $Name:  $
// Author: Hui Shao
// Desc  : Define float stream window
//
// Revision History: 
// ---------------------------------------------------------------------------
// $Log: /ZQProjs/TianShan/Stream/VPumps.h $
// ===========================================================================

#ifndef __streamcraft_ShmPage_H__
#define __streamcraft_ShmPage_H__

#include "ZQ_common_conf.h"
#include "SharedPage.h"
#include "PageFile.h"

#include "FileLog.h"
#include "TimeUtil.h"
#include "UDPSocket.h"

#include <vector>
#include <queue>
#include <map>

namespace syscheme {
namespace StreamCraft {

///ShmPage Buffer 
/// head(512byte:  key) + data(2M)

// -----------------------------
// class ShmPage
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
class ShmPage : public SharedPage
{
public:
	typedef ZQ::common::Pointer<ShmPage> Ptr;
	typedef std::vector < Ptr > List;

	// only hatched by the SegementManager
	ShmPage(PageManager& sm, PageDescriptor& bufd, const std::string& uriOrigin, int64 offsetOrigin, int headLength = HEADLEN_SegmentFile);

	virtual ~ShmPage();

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

	virtual uint8* ptr();

private:

	void* mmap();
	void munmap();

public:

	uint          _offsetInFd;
};

#define METHOD_ALLOCATE		"rpcAllocate"
#define METHOD_FREE		    "rpcFree"
#define CLEARUSED_INTERVAL  (30 * 1000)   //30s

#define SHMPATH  "/dev/shm/" //the path to create shm mem

class ShmSegmentTimer;

// -----------------------------
// class ShmPages_Local
// -----------------------------
// the ShmPage with DiskCahe backed
class ShmPages_Local : public PageManager // TODO: make CPMICLIENT(StreamCPMI::DiskCache) as a member _disk_backed instead of inherit, public CPMICLIENT(StreamCPMI::DiskCache)
{
	friend class ShmSegmentTimer;
public:

	ShmPages_Local(ZQ::common::Log& log, ZQ::sloop::InterruptibleLoop& loopEx, uint maxSegs, int cacheLoadTimeout = 5000, int cacheFlushTimeout = 1000 * 60 * 5, int allocateWaitTimout = 500, const std::string& prefix="SHMSEG_", const std::string& diskCacheServerPipeName = "/var/run/TianShan/LIPC/DiskCache", bool bClear = false);
	virtual ~ShmPages_Local();

	///allocate a segment with key as the purpsoe
	///implement  PageManager
	virtual SharedPage::Ptr allocate(const std::string& uriOrigin, int64 offsetOrigin);
	virtual void onFull(SharedPage::Ptr seg);
	virtual void updateSegment(SharedPage::Ptr seg);
	uint32 getMaxSegs() { return _maxSegs; };
	std::string fd2SegmentName(int fd);

	void setVerbosity(uint16 verbose = (0 | ZQ::common::Log::L_ERROR)) { PageManager::setVerbosity(verbose); CPMICLIENT(StreamCPMI::DiskCache)::setVerbosity(verbose); }
//	void close();
	typedef struct _Statistics
	{
		int cHitMem;
		int cHitDisk;
		int cMissed;
		int total;
		int free;
		int used;
		int diskcache;

		_Statistics()
		{
			cHitMem   = 0;
			cHitDisk  = 0;
			cMissed   = 0;
			total     = 0;
			free      = 0;
			used      = 0;
			diskcache = 0;
		}
	} Statistics;

	Statistics getStatistics();
	void resetStatistics();

protected:

	///implement PageManager
	virtual void free(SharedPage::Ptr seg);
	virtual void free(SharedPage::PageDescriptor bufd);
	virtual size_t freeSize();

	size_t _recycleFd(int freeFd, const char* reason = "");
    
	///implement  DiskCacheClient
	virtual void OnSegmentLoaded(SegmentDescriptor& seg, int err);
	virtual void OnSegmentFlushed(SegmentDescriptor& seg, int err);

	///overwrite CPMIClient
	virtual void stepDo(bool byHeartbeat);

	// virtual void OnConnected(ZQ::eloop::Handle::ElpeError status);
	// virtual void onError( int error,const char* errorDescription);
	// virtual void OnClose();
	// bool isConnected() { return _connOK;};

	///////////////////////////////////////////////////////////////////////////////

	class ShmFile : ZQ::common::SharedObject
	{
	public:
		typedef ZQ::common::Pointer<ShmFile> Ptr;
	public:
		ShmFile(){ _cHatched.set(0);}
		ShmFile(const std::string& pathName, const SharedPage::PageDescriptor& bufd)
			: _pathName(pathName), _bufd(bufd)
		{
			_cHatched.set(0);
		}

		virtual ~ShmFile() {}

		void updateHeader();

		int getHatched()
		{
			return _cHatched.get();
		}

		/*
		 ShmFile(const ShmFile& shmFile)
		 {
		 	_bufd = shmFile._bufd;
		 	_pathName = shmFile._pathName;
		 	_cHatched = shmFile._cHatched;
		 }
		  ShmFile& operator=(const ShmFile shmFile)
		  {
			  //copy is not permitted
			  _bufd = shmFile._bufd;
			  _pathName = shmFile._pathName;
			  _cHatched = shmFile._cHatched;
			  return *this;
		  }
		 */
	 public:
		std::string _pathName;
		SharedPage::PageDescriptor _bufd;
		ZQ::common::AtomicInt _cHatched;
	};

protected:
	typedef std::map<int, ShmFile>  ShmFileMap; // map of fd to ShmFile
	// NOTE: when Evictor involves, the above ShmFileMap should be
	// std::map<int, pathName> ShmFileMap;
	// and access to the ShmFile should become _evictorShmFile.locate(ShmFileMap[fd])

	typedef std::vector<int> FdSet; // TODO: should be std::set<int> to avoid duplicate
	typedef std::list<int> FreeList;

	ShmFileMap       _shmFileMap; // the base data structure manages the shm fIles
	FreeList         _freeList;

	typedef struct  _UsedInfo
	{
		FdSet usedFds;
		int   cacheLoadFd;
		bool  bReadingFromDiskCache;
		int64 timeStampRequest;
		int64 timeStampResponse;

		_UsedInfo()
		{
			timeStampRequest = 0;
			timeStampResponse = 0;
			bReadingFromDiskCache = false;
			cacheLoadFd = -1;
		}
	} UsedInfo;
	// the bi-direction indexes of the loaded playload, between key and fd
	typedef std::map<std::string, UsedInfo> KeyToFdIndex; // map of PageDescriptor::key to UsedInfo
	// typedef std::map<int, std::string>   FdToKeyIndex; // map of fd to key
	KeyToFdIndex     _usedIdxKeyToFd;
	// FdToKeyIndex     _loadIdxFdToKey;
	ZQ::common::Mutex _lkShm;

	std::string _prefix;
	uint32      _maxSegs;

	// std::string _diskCacheServerPipeName;
	// bool        _connOK;
	// int64       _stampConnectingDiskCache;
	// bool          _bDisabledDiskCache;

	int64       _stampClearUsed;

	int               _cacheLoadTimeout; //ms
	int               _allocateWaitTimout;
	int	              _cacheFlushTimeout;

	Statistics    _shmStatistics;
	FdSet         _fdLists;//for test

private:
	SharedPage::Ptr hatch(ShmFile& shmFile, const std::string& uriOrigin, int64 offsetOrigin);
	bool _newNextSeg(); // thread unsafe
	int clearShmMem(const char* pathName,const char* fitName);//used for clean the redundant shm before newSeg
	void clear();

	int  _findFreeFd(const std::string& key, const std::string& uriOrigin, int64 offsetOrigin);

	typedef enum 
	{
      MEMORY_HIT = 0, //allocated from usedmap
	  DISKCACHE_HIT,  //read from disk cache
	  MISSED_HIT        //allocate from freelist
	} StatisticsType;

	void _count(StatisticsType type);
};

}} //end namesapce
#endif // __streamcraft_ShmPage_H__

