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
// NOTE: How to increase or decrease /dev/shm filesystem size on Linux
//  1) edit /etc/fstab to adjust the line of /dev/shm, e.g. 10GB:
//          tmpfs      /dev/shm      tmpfs   defaults,size=10g   0   0
//  2) to make change effective immediately, run the following command to remount:
//			mount -o remount /dev/shm
// Revision History: 
// ---------------------------------------------------------------------------
// $Log: /ZQProjs/TianShan/Stream/VPumps.h $
// ===========================================================================

#include "ShmPage.h"
#include <algorithm>

extern "C" {
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef ZQ_OS_LINUX
#  include <unistd.h>
#	include <dirent.h>
#  include <sys/mman.h>
#  include <sys/sendfile.h>
#endif
}

#define RECONNECT_INTERVAL_DEFAULT  (10 * 1000) //10s

namespace syscheme {
namespace StreamCraft {
	
#define min(a,b)    (((a) < (b)) ? (a) : (b))
#define MAPSET(_MAPTYPE, _MAP, _KEY, _VAL) if (_MAP.end() ==_MAP.find(_KEY)) _MAP.insert(_MAPTYPE::value_type(_KEY, _VAL)); else _MAP[_KEY] = _VAL
#define JSON_HAS(OBJ, CHILDNAME) (OBJ.isObject() && OBJ.isMember(CHILDNAME)) // for jsoncpp bug that didn't test type in isMember()

//#define TRACE_LEVEL_FLAG (PageManager::_verboseFlags & FLG_TRACE)
// #define INFO_LEVEL_FLAG (PageManager::_verboseFlags & FLG_INFO)

// -----------------------------
// class ShmPage
// -----------------------------
ShmPage::ShmPage(PageManager& sm, PageDescriptor& bufd, const std::string& uriOrigin, int64 offsetOrigin, int headLength)
: LargeContentPage(sm, bufd, uriOrigin, offsetOrigin, headLength), _offsetInFd(HEADLEN_SegmentFile)
{
	if (_bufd.fd > 0)
		lseek(_bufd.fd, _offsetInFd, SEEK_SET);
}

ShmPage::~ShmPage()
{
	munmap();
}

int ShmPage::fill(int fdSource, uint length, int startOffset)
{
//	int tempStartoffset= startOffset, tempOffsetId= _offsetInFd, tempBufLen = _bufd.length;

	if(_bufd.flags & sfReadOnly)
		return 0;

	if (_bufd.fd <=0)
		return -2;

	if (fdSource <0)
		return -1;

	if (startOffset >= (int)_bufd.capacity)
		return 0;

	//adjust offset in file
	if (startOffset < 0)
		startOffset = _offsetInFd;
	else
		startOffset += HEADLEN_SegmentFile;

	size_t len = min(length, (_bufd.capacity + HEADLEN_SegmentFile) - startOffset);

	// copy data from fdSource to _bufd.fd
	if ( _offsetInFd != startOffset)
	{	
		_offsetInFd = startOffset;
		lseek(_bufd.fd, _offsetInFd, SEEK_SET);
	}

	ssize_t nbytes = sendfile(_bufd.fd, fdSource, NULL, len);

	_offsetInFd += nbytes;

	if((_offsetInFd - HEADLEN_SegmentFile) >  _bufd.length)
	{
		_bufd.length = _offsetInFd - HEADLEN_SegmentFile;
	}

//	printf("fill() startOffset[%d==>%d] , _offsetInFd =[%d==>%d], _bufd.length [%d==>%d] wirtenLen=%d\n",
//		tempStartoffset, startOffset, tempOffsetId, _offsetInFd, tempBufLen, _bufd.length , nbytes);

	return nbytes;
}

// fill data from the source to this segment by memory address
//@param source, the memory address of the source
//@param length, the number of bytes to read from the source and fill into the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start fill
//@return the number of bytes has successfully filled into the segment
int ShmPage::fill(uint8* source, uint length, int startOffset)
{ 
 //    int tempStartoffset= startOffset, tempOffsetId= _offsetInFd, tempBufLen = _bufd.length;

	 if(_bufd.flags & sfReadOnly)
		 return 0;

	if (_bufd.fd <= 0)
		return -2;

	if (NULL == source)
		return -3;

	if (startOffset >= (int)_bufd.capacity)
		return 0;

	if (NULL == mmap()|| _bufd.ptr == NULL)
		return -1;

	//adjust offset in file
	if (startOffset < 0)
		startOffset = _offsetInFd;
	else 
	{
		startOffset += HEADLEN_SegmentFile;
		_offsetInFd =  startOffset;
	}
		
	int len = (_bufd.capacity + HEADLEN_SegmentFile) - startOffset;
	if (len > (int)length)
		len = length;

	memcpy(_bufd.ptr + _offsetInFd, source, len);

	_offsetInFd += len;

	if((_offsetInFd - HEADLEN_SegmentFile) >  _bufd.length)
	{
		_bufd.length = _offsetInFd - HEADLEN_SegmentFile;
	}

//	printf("fill() startOffset[%d==>%d] , _offsetInFd =[%d==>%d], _bufd.length [%d==>%d] wirtenLen=%d\n",
//		tempStartoffset, startOffset, tempOffsetId, _offsetInFd, tempBufLen, _bufd.length , len);

	return len;
}

// output data from the segement to a target by fd
//@param fdTarget, the fd of the target
//@param length, the number of bytes to read from the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start flushing
//@return the number of bytes has successfully filled into the segment
int ShmPage::flush(int fdTarget, uint length, int startOffset)
{	
//	int tempStartoffset= startOffset, tempOffsetId= _offsetInFd, tempBufLen = _bufd.length;

	if (_bufd.fd <=0)
		return -2;

	if (fdTarget <0)
		return -1;

	if (startOffset >= (int)_bufd.capacity)
		return 0;

	//adjust offset in file
	if (startOffset < 0)
		startOffset = _offsetInFd;
	else
		startOffset += HEADLEN_SegmentFile;

	size_t len = min(length, (_bufd.capacity + HEADLEN_SegmentFile) - startOffset);

	// copy data from fdSource to _bufd.fd
	if ( _offsetInFd != startOffset)
	{	
		_offsetInFd = startOffset;
		lseek(_bufd.fd, _offsetInFd, SEEK_SET);
	}

	ssize_t nbytes = sendfile(fdTarget, _bufd.fd, NULL, len);
	if (nbytes > 0)
		_offsetInFd += nbytes;

//	printf("flush() startOffset[%d==>%d] , _offsetInFd =[%d==>%d], _bufd.length [%d==>%d] ReadLen=%d\n",
//		tempStartoffset, startOffset, tempOffsetId, _offsetInFd, tempBufLen, _bufd.length , len);

	return nbytes;
}

// output data from the segement to a target by memory address
//@param target, the memory address of the target
//@param length, the number of bytes to read from the source and fill into the target
//@param startOffset, if >=0, to specify the offset within the segment where to start fill
//@return the number of bytes has successfully filled into the segment
int ShmPage::flush(uint8* target, uint length, int startOffset)
{
//	int tempStartoffset= startOffset, tempOffsetId= _offsetInFd, tempBufLen = _bufd.length;

	if (_bufd.fd <=0)
		return -2;

	if (NULL == target)
		return -3;

	if (startOffset >= (int)_bufd.capacity)
		return 0;

	if (NULL == mmap())
		return -1;

	//adjust offset in file
	if (startOffset < 0)
		startOffset = _offsetInFd;
	else 
	{
		startOffset += HEADLEN_SegmentFile;
		_offsetInFd =  startOffset;
	}

	if((int)_bufd.length < (startOffset - HEADLEN_SegmentFile))
		return -1;

	int len = _bufd.length - (startOffset - HEADLEN_SegmentFile);

	if (len > (int)length)
		len = length;

	memcpy(target, _bufd.ptr + startOffset, len);

	_offsetInFd += len;

//	printf("flush() startOffset[%d==>%d] , _offsetInFd =[%d==>%d], _bufd.length [%d==>%d] ReadLen=%d\n",
//		tempStartoffset, startOffset, tempOffsetId, _offsetInFd, tempBufLen, _bufd.length , len);

	return len;
}

void* ShmPage::mmap()
{ 
	lseek(_bufd.fd, 0, SEEK_SET);

	if (NULL == _bufd.ptr) 
		_bufd.ptr = (uint8 *)::mmap(NULL, _bufd.capacity + HEADLEN_SegmentFile, PROT_WRITE|PROT_READ, MAP_SHARED, _bufd.fd, 0);

	if (_bufd.ptr == MAP_FAILED)
	{
		//	printf("error %d \n", errno);
		_bufd.ptr = NULL;
	}

	return _bufd.ptr;
} 

void ShmPage::munmap()
{
	if (NULL == _bufd.ptr)
		return;

	::munmap(_bufd.ptr, _bufd.capacity + HEADLEN_SegmentFile);
	_bufd.ptr =NULL;
}

uint8* ShmPage::ptr()
{
	if (NULL == mmap()|| _bufd.ptr == NULL)
		return NULL;

	return (_bufd.ptr + HEADLEN_SegmentFile);
}


// -----------------------------
// class ShmPages_Local
// -----------------------------
#define TRACE(LEVEL, ...)      WRAPPEDLOG(PageManager::_lw, LEVEL, __VA_ARGS__)

ShmPages_Local::ShmPages_Local(ZQ::common::Log& logger, ZQ::sloop::InterruptibleLoop& loopEx, uint maxSegs, int cacheLoadTimeout, int cacheFlushTimeout, int allocateWaitTimout, const std::string& prefix, const std::string& diskCacheServerPipeName, bool bClear)
: PageManager(logger, loopEx), CPMICLIENT(DiskCache)(loopEx, logger, diskCacheServerPipeName, MAX(cacheLoadTimeout, cacheFlushTimeout) +1000),
  _prefix(prefix), _maxSegs(maxSegs), _cacheLoadTimeout(cacheLoadTimeout), _cacheFlushTimeout(cacheFlushTimeout), _allocateWaitTimout(allocateWaitTimout)
  //, _connOK(false),_diskCacheServerPipeName(diskCacheServerPipeName), _bDisabledDiskCache(bDisabledDiskCache)
{
	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "ShmPages_Local(), DiskCache[%s]"), diskCacheServerPipeName.c_str());
	clear();

	if (diskCacheServerPipeName.empty())
		CPMICLIENT(DiskCache)::disconnect();

	if (_prefix.empty())
		_prefix = "SHMSEG_";
	//clean the shm may used by the Engine
	if(bClear)
		clearShmMem(SHMPATH,_prefix.c_str());

	ZQ::common::MutexGuard g(_lkShm);
	for (size_t i=0; i < maxSegs; i++)
	{	
		if(false == _newNextSeg())
			break;
	}

	_shmStatistics.total = maxSegs;
	_shmStatistics.free = (int)_freeList.size();

	_stampClearUsed = ZQ::common::now();
	// _stampConnectingDiskCache =  ZQ::common::now();
}

void ShmPages_Local::clear()
{
	ZQ::common::MutexGuard g(_lkShm);
	_shmFileMap.clear();
	_freeList.clear();
	_usedIdxKeyToFd.clear();
}

int ShmPages_Local::clearShmMem(const char* pathName,const char* fitName)
{
	int delCount = 0;
	DIR *dirptr=NULL;
	struct dirent *entry;
	std::string filePath;
	filePath = pathName;
	if(pathName[strlen(pathName)-1] != '/')
		filePath += "/";

	if((dirptr = opendir(pathName))==NULL)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "clearShmMem() open clean dir[%s] failed"), pathName);
		return delCount;
	}

	std::string filename;
	std::string tmpName = ".";
	tmpName += fitName;
	while(NULL != (entry = readdir(dirptr)))
	{
		//condition: .fitName or fitName
		if(0 == strncmp(entry->d_name,tmpName.c_str(),tmpName.size()));
		else if(0 == strncmp(entry->d_name,fitName,strlen(fitName)));
			else continue;

		filename = filePath + std::string(entry->d_name);
		//::shm_unlink(filename.c_str());

		unlink(filename.c_str());//delete the exist shm file
		++delCount;//count the delete num

		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "clearShmMem() clean the shm [%s] before newSeg"),filename.c_str());
	}

	closedir(dirptr);
	return delCount;
}

ShmPages_Local::~ShmPages_Local()
{
	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "~ShmPages_Local()"));

	ZQ::common::MutexGuard g(_lkShm);
	_usedIdxKeyToFd.clear();
	_freeList.clear();
	ShmFileMap::iterator itorShmFile = _shmFileMap.begin();
	while (itorShmFile != _shmFileMap.end())
	{
		int fd = itorShmFile->first;
		::close(fd);
		::shm_unlink(itorShmFile->second._pathName.c_str());
		itorShmFile++;
	}
	_shmFileMap.clear();
}

void ShmPages_Local::ShmFile::updateHeader()
{
	ZQ::memory::LargeContentPage::PageDescriptor segBufd;
	ZQ::memory::PageFile::Header fheader;
	segBufd.fd = _bufd.fd;
	segBufd.flags = _bufd.flags;
	segBufd.key = _bufd.key;
	segBufd.length = _bufd.length;
	segBufd.capacity = _bufd.capacity;
	segBufd.stampAsOfOrigin = _bufd.stampAsOfOrigin;

	ZQ::memory::PageFile::IOError err = ZQ::memory::PageFile::bufDescToHeader(segBufd, fheader);

	if(err == ZQ::memory::PageFile::eOK)
	{
		// write the file header
		lseek(_bufd.fd, 0, SEEK_SET);
		if (write(_bufd.fd, &fheader, sizeof(fheader)) < sizeof(fheader))
			;//log(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "updateHeader() shmFile[%s(%d)] caught error[%d]"),  _pathName.c_str(), _bufd.fd, err);
	}
}

std::string ShmPages_Local::fd2SegmentName(int fd)
{ 
	ZQ::common::MutexGuard g(_lkShm);

	ShmFileMap::iterator itor = _shmFileMap.find(fd);
	if(itor != _shmFileMap.end())
		return itor->second._pathName;

   return "";
}

// allocate a segment with key as the purpsoe
LargeContentPage::Ptr ShmPages_Local::allocate(const std::string& uriOrigin, int64 offsetOrigin)
{
	int64 lStart = ZQ::common::now();
	int64 tmpOffsetOfOrigin =  SEG_IDX_OF_OFFSET(offsetOrigin);
	std::string key = PageManager::genShmkey(uriOrigin, tmpOffsetOfOrigin);
	offsetOrigin = SEG_STARTOFFSET(tmpOffsetOfOrigin);

	int freeFd = 0;

	LargeContentPage::Ptr  streamSegmentPtr;
	ZQ::common::MutexGuard g(_lkShm);

	KeyToFdIndex::iterator itUsed = _usedIdxKeyToFd.find(key);

	if (_usedIdxKeyToFd.end() == itUsed)
	{
		// case 1. the key has never been refered, then hatch a new one
		//   allocateFromFree()
		//   diskcache->load(seg)
		//   return NULL;
		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s] offsetOrigin[%lld] new key ever heard"), key.c_str(), offsetOrigin);

		/// find a free segment
		freeFd = _findFreeFd(key, uriOrigin, offsetOrigin);

		if(freeFd <= 0)
			return NULL;

		//ensure  _usedIdxKeyToFd map has key
		KeyToFdIndex::iterator itCacheLoad = _usedIdxKeyToFd.find(key);
		if(itCacheLoad == _usedIdxKeyToFd.end())
		{
			//_freeList.push_back(freeFd);
			_recycleFd(freeFd, "allocate() internal error");
			return NULL;
		}

		UsedInfo& useInfo =  itCacheLoad->second;

		///read from diskCache, update timeStampRequest cacheLoadFd;
		SegmentDescriptor segD;
		segD.fd = freeFd;
		segD.key = key;
		segD.length = 0;
		segD.capacity = _shmFileMap[freeFd]._bufd.capacity;
		segD.pathName = fd2SegmentName(segD.fd);
		segD.flags = 0;

		if (isConnected() && call_loadSegment(segD, _cacheLoadTimeout) >0)
		{	
			useInfo.bReadingFromDiskCache = true;
			useInfo.timeStampRequest = ZQ::common::now();
			useInfo.timeStampResponse = 0;
			useInfo.cacheLoadFd = freeFd;

			TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "allocate() key[%s] requested diskcache to fill shm[%s(%d)], took %dms"), key.c_str(), fd2SegmentName(freeFd).c_str(),freeFd,(int)(ZQ::common::now() - lStart));
			return NULL;
		}

		//if send loadSegment request failed, return hatch freefd shmsegment, add freeFd to _usedIdxKeyToFd map, sign the key is not read from diskcache
		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "allocate() key[%s] allocated new shm[%s(%d)] max%d/used%d/free%d, took %dms"),
			key.c_str(), _shmFileMap[freeFd]._pathName.c_str(), freeFd, _maxSegs, _usedIdxKeyToFd.size(), _freeList.size(),(int)(ZQ::common::now() - lStart));

		_count(MISSED_HIT);

		useInfo.usedFds.push_back(freeFd);
		useInfo.cacheLoadFd = -1;
		useInfo.bReadingFromDiskCache = false;
		useInfo.timeStampRequest = 0;
		useInfo.timeStampResponse = 0;
		streamSegmentPtr =  hatch(_shmFileMap[freeFd], uriOrigin, offsetOrigin);
		return streamSegmentPtr;
	}

	// case 2. the same key has been ever hatched
	///find recent full shmFile

	UsedInfo& useInfo =  itUsed->second;
	int recentFullFd = -1;
	int64 stampAsOfOrigin = 0;

	int freeFdFullFd = 0;

	for (FdSet::iterator itFd = useInfo.usedFds.begin(); itFd != useInfo.usedFds.end(); itFd++)
	{
		int& fd = *itFd;
		ShmFileMap::iterator itShm = _shmFileMap.find(fd);
		if(itShm == _shmFileMap.end())
			continue;

		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s]fd[%d] stampAsOfOrigin[%lld]recentFullFd[%d]  freeFdFullFd[%d] shm([%u/%u]stampAsOfOrigin[%lld])"),
			key.c_str(), fd, stampAsOfOrigin, recentFullFd, freeFdFullFd, itShm->second._bufd.length, itShm->second._bufd.capacity, itShm->second._bufd.stampAsOfOrigin  );

		//    case 2.1 there is one fully filled, directly take it by duplicate-hatch a Segment
		if (itShm->second._bufd.length == itShm->second._bufd.capacity && itShm->second._bufd.stampAsOfOrigin > stampAsOfOrigin)
		{
			if(freeFdFullFd > 0)
			{
				TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s] add to freelist[%d], max%d/used%d/free%d, took %dms"),
					key.c_str(), freeFdFullFd,  _maxSegs, _usedIdxKeyToFd.size(), _freeList.size(),(int)(ZQ::common::now() - lStart));

				_freeList.push_back(freeFdFullFd);
				//_recycleFd(freeFdFullFd, "allocate() older");
				freeFdFullFd = 0;
			}
			recentFullFd = fd;
			stampAsOfOrigin = itShm->second._bufd.stampAsOfOrigin;

			// ensure it is no more left in the _freeList
			if (0 == itShm->second._cHatched.get())
			{
				/*			for (FreeList::iterator itF = _freeList.begin(); itF != _freeList.end();)
				{
				if (*itF == itShm->second._bufd.fd)
				{
				if(INFO_LEVEL_FLAG)
				log(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "allocate() key[%s] remove from freelist[%d], max%d/used%d/free%d, took %dms"),
				key.c_str(), *itF, freeFd, _maxSegs, _usedIdxKeyToFd.size(), _freeList.size(),(int)(ZQ::common::now() - lStart));

				freeFdFullFd = fd;
				itF = _freeList.erase(itF);
				break;
				}
				itF++;
				}*/

				FreeList::iterator itorFree = std::find(_freeList.begin(), _freeList.end(), fd);
				if (itorFree != _freeList.end())
				{
					TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s] remove from freelist[%d], max%d/used%d/free%d, took %dms"),
						key.c_str(), fd, _maxSegs, _usedIdxKeyToFd.size(), _freeList.size(),(int)(ZQ::common::now() - lStart));

					freeFdFullFd = fd;
					_freeList.erase(itorFree);
				}
			}
		}
	}

	if(recentFullFd > 0  && _shmFileMap.find(recentFullFd) != _shmFileMap.end())
	{
		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "allocate() key[%s] resusing previous fully-filled shm[%s(%d)],flags[%d] shm-usage[K%d+F%d/%d] took %dms"),
			key.c_str(),_shmFileMap[recentFullFd]._pathName.c_str(), _shmFileMap[recentFullFd]._bufd.fd,_shmFileMap[recentFullFd]._bufd.flags, _usedIdxKeyToFd.size(), _freeList.size(), _maxSegs, (int)(ZQ::common::now() - lStart));

		streamSegmentPtr =  hatch(_shmFileMap[recentFullFd], uriOrigin, offsetOrigin);
		if(streamSegmentPtr.get()  != NULL)
			streamSegmentPtr->setReadOnly();

		_count(MEMORY_HIT);
		return streamSegmentPtr;
	}

	// case 2.2 none of the segment has been fully filled, then hatch a new one from the free list
	// case 2.2a there is a segment being loaded, and elapsed time < timeout, return NULL for the next retry
	//Reading from diskcache, wait
	if (useInfo.bReadingFromDiskCache && useInfo.timeStampRequest > 0 && useInfo.timeStampResponse <= 0)
	{
		int64 cacheLoadTime = ZQ::common::now() - useInfo.timeStampRequest;
		if (cacheLoadTime < _allocateWaitTimout)
		{
			TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s] waiting diskcache on fd[%d], elapsed %dms"), key.c_str(), useInfo.cacheLoadFd,(int)(ZQ::common::now() - lStart));
			return NULL;   
		}
	}

		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "allocate() key[%s] none of %dfds has fully filled, expecting a new fd, readingFromDiskCache[%s: %d,%lld-%lld,took %dms] shm-usage[K%d+F%d/%d]"),
		key.c_str(), itUsed->second.usedFds.size(), useInfo.bReadingFromDiskCache ? "YES" : "NO", useInfo.cacheLoadFd, useInfo.timeStampRequest, useInfo.timeStampResponse, (int)(ZQ::common::now() - useInfo.timeStampRequest),  _usedIdxKeyToFd.size(), _freeList.size(), _maxSegs);

	// case 2.2b there is a segment being loaded but timeout, continue to the new segement allocation from the freelist
	freeFd = _findFreeFd(key, uriOrigin, offsetOrigin);
	if(freeFd <= 0)
		return NULL;

	if((PageManager::getVerbosity()&0xf) <= ZQ::common::Log::L_INFO)
	{
		std::string usedFd;
		for (int i = 0; i < _usedIdxKeyToFd[key].usedFds.size(); i++)
		{
			char buf[10];
			memset(buf, 0, sizeof(buf));
			itoa(_usedIdxKeyToFd[key].usedFds[i], buf, 10);
			usedFd += buf + std::string("; ");
		}

		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "allocate() key[%s:<fds:%s>] allocated new shm[%s(%d)], shm-usage[K%d+F%d/%d], took %dms"),
		key.c_str(), usedFd.c_str() , _shmFileMap[freeFd]._pathName.c_str(), freeFd, _usedIdxKeyToFd.size(), _freeList.size(), _maxSegs, (int)(ZQ::common::now() - lStart));
	}

	streamSegmentPtr =  hatch(_shmFileMap[freeFd], uriOrigin, offsetOrigin);

	_count(MISSED_HIT);
	return streamSegmentPtr;
}

int  ShmPages_Local::_findFreeFd(const std::string& key, const std::string& uriOrigin, int64 offsetOrigin)
{
	int lStart = ZQ::common::now();
	// allocate from the free list
	ShmFileMap::iterator itShmFile = _shmFileMap.end();
	int fd = INVALID_FD;
	while (!_freeList.empty())
	{
		fd = _freeList.front(); 
		_freeList.pop_front();

		itShmFile = _shmFileMap.find(fd);
		if (INVALID_FD == fd || _shmFileMap.end() == itShmFile)
		{
			// this must be a bug
			TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "_findFreeFd() key[%s] fd[%d] unknown"), key.c_str(), fd);
			continue;
		}

		break;
	}

	if (_shmFileMap.end() == itShmFile) //shm is full, can not allocate
	{
		TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "_findFreeFd() key[%s] run out of shm took %dms"), key.c_str(), (int)(ZQ::common::now() - lStart));
		return -1;
	}

	// okay, we have find a suitable ShmFile from the _freeList
	// ensure it is not linger at any _usedMap
	std::string oldkey = itShmFile->second._bufd.key;
	KeyToFdIndex::iterator itOldK2F = _usedIdxKeyToFd.find(oldkey);
	if (_usedIdxKeyToFd.end() != itOldK2F)
	{
		FdSet& fds = itOldK2F->second.usedFds;
		FdSet::iterator itorFds = std::find(fds.begin(), fds.end(), itShmFile->second._bufd.fd);
		if(itorFds != fds.end())
			fds.erase(itorFds);

		if (itOldK2F->second.usedFds.empty() &&  !itOldK2F->second.bReadingFromDiskCache) 
		{
			_usedIdxKeyToFd.erase(oldkey);

			TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "_findFreeFd() remove oldkey[%s] shm[%s(%d)]"),	(itOldK2F->first).c_str(), itShmFile->second._pathName.c_str(), itShmFile->second._bufd.fd);
		}
	}

	// reset some attributres
	itShmFile->second._bufd.key = key;
	itShmFile->second._bufd.flags = 0;
	itShmFile->second._bufd.ptr = NULL;
	itShmFile->second._bufd.length = 0;
	itShmFile->second._bufd.stampAsOfOrigin = 0;
	itShmFile->second._bufd.capacity = SEGMENT_SIZE;
	itShmFile->second.updateHeader();

	KeyToFdIndex::iterator itUsed = _usedIdxKeyToFd.find(key);
	if (_usedIdxKeyToFd.end() == itUsed)
	{
		// about the above case 2, add a new BusyList into the map
		//FdSet fdset;
		//fdset.push_back(itShmFile->second._bufd.fd);
		UsedInfo usedInfo;
		usedInfo.timeStampRequest = 0;
		usedInfo.timeStampResponse = 0;
		usedInfo.bReadingFromDiskCache = false;
		usedInfo.cacheLoadFd = -1;
		usedInfo.usedFds.clear();
		_usedIdxKeyToFd[key] = usedInfo;
	}
	else
		itUsed->second.usedFds.push_back(itShmFile->second._bufd.fd); // add the new fd into the BusyList

	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "_findFreeFd() key[%s] found free shm[%s(%d)] shm-usage[K%d+F%d/%d] took %dms"),
		key.c_str(), itShmFile->second._pathName.c_str(), itShmFile->second._bufd.fd,  _usedIdxKeyToFd.size(), _freeList.size(), _maxSegs, (int)(ZQ::common::now() - lStart));

	return itShmFile->second._bufd.fd;
}

LargeContentPage::Ptr ShmPages_Local::hatch(ShmFile& shmFile, const std::string& uriOrigin, int64 offsetOrigin)
{
	shmFile._cHatched.inc();
	return hatchSegObj<ShmPage>(shmFile._bufd, uriOrigin, offsetOrigin); // leave the bufd.key as it was assigned
}

void ShmPages_Local::free(LargeContentPage::PageDescriptor bufd)
{	
	int64 lStart = ZQ::common::now();
#ifdef _DEBUG
	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "free() key[%s] shm[%s(%d)] len[%d/%d]"),  bufd.key.c_str(),fd2SegmentName(bufd.fd).c_str() ,bufd.fd, bufd.length, bufd.capacity);
#endif // _DEBUG

	ZQ::common::MutexGuard g(_lkShm);

	ShmFileMap::iterator itShmFile = _shmFileMap.find(bufd.fd);
	if (_shmFileMap.end() == itShmFile)
	{
		// this must be a bug
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "free() key[%s] ShmFile not found by fd[%d]"), bufd.key.c_str(), bufd.fd);
		return;
	}

	itShmFile->second._bufd.length = bufd.length; // update only at free() is not so time effective, need to enhance

	if (!itShmFile->second._cHatched.decThenIfZero())
	{
		// some other Segment still reference to this ShmFile, do nothing and leave what it was
		// see also: the case 2.1 in allocate()
		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "free() key[%s] shm[%s(%d)] has more other reference, leave it, took %dms"), 
			itShmFile->second._bufd.key.c_str(), itShmFile->second._pathName.c_str(), itShmFile->second._bufd.fd, (int)(ZQ::common::now() - lStart));
		return;
	}

	// when reach here, it must be the last Segment to free

	// NO MORE withdraw from from the BusyMap, leave allocate to ensure the resused fd in _freeList
	//BusyMap::iterator itUsed = _busyMap.find(key);
	//if (_busyMap.end() != itUsed)
	//	itUsed->second.fdset.erase(itShmFile->bufd.fd);

	// put it back to the _freeList
	//_freeList.push_back(itShmFile->second._bufd.fd);
	_recycleFd(itShmFile->second._bufd.fd, "free()");

/*
	//if buffer not full, remove it from user map
	if(bufd.flags & sfDirty)
	{
		KeyToFdIndex::iterator itRemove = _usedIdxKeyToFd.find(bufd.key);
		if (_usedIdxKeyToFd.end() != itRemove)
		{
			FdSet& fds = itRemove->second.usedFds;
			FdSet::iterator itorFds = std::find(fds.begin(), fds.end(), itShmFile->second._bufd.fd);
			if(itorFds != fds.end())
				fds.erase(itorFds);

			if (itRemove->second.usedFds.empty())
				_usedIdxKeyToFd.erase(itRemove);
		}

		itShmFile->second._bufd.key = "";
		itShmFile->second._bufd.ptr = NULL;
		itShmFile->second._bufd.length = 0;
		itShmFile->second._bufd.stampAsOfOrigin = 0;
		itShmFile->second.updateHeader();
	}
*/
	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "free() key[%s] at shm[%s(%d)] free-ed: %u/%ubytes, shm-usage[K%d+F%d/%d], took %dms"),
		itShmFile->second._bufd.key.c_str(), itShmFile->second._pathName.c_str(), itShmFile->second._bufd.fd, itShmFile->second._bufd.length, itShmFile->second._bufd.capacity, _usedIdxKeyToFd.size(), _freeList.size(), _maxSegs, (int)(ZQ::common::now() - lStart));
}

void ShmPages_Local::free(LargeContentPage::Ptr seg)
{
	if (!seg)
		return;

	LargeContentPage::PageDescriptor bufd;
	bufd.key =  seg->key();
	if(seg->isDirtyLeading())
	{
		bufd.length  = 0;
		bufd.stampAsOfOrigin = 0;
	}
	else
	{
		bufd.length = seg->getLength();
		bufd.stampAsOfOrigin = seg->getstampAsOfOrigin();
	}

	bufd.capacity = seg->getCapacity();
	return free(bufd);
}

bool ShmPages_Local::_newNextSeg() // thread unsafe
{
	size_t idx = _shmFileMap.size();
	if (idx >= _maxSegs)
		return false;

	char segname[64];
	snprintf(segname, sizeof(segname)-2, ".%s%04X", _prefix.c_str(), idx);

#define OPEN_FLAG (O_RDWR|O_CREAT)
#define OPEN_MODE 00600

	int fd = shm_open(segname,  OPEN_FLAG, OPEN_MODE);
	if (fd <0)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "_newNextSeg() shm_open failed seg[%d]->fd[%d]:%s]"), idx, fd, segname);
		return false;
	}
		
	if (ftruncate(fd, HEADLEN_SegmentFile + SEGMENT_SIZE) == -1)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "_newNextSeg() ftruncate failed seg[%d]->fd[%d]:%s]"), idx, fd, segname);
		return false;
	}

	char *Tmp = new char[HEADLEN_SegmentFile + SEGMENT_SIZE];//(char*)malloc(HEADLEN_SegmentFile + SEGMENT_SIZE);
	memset(Tmp,0,HEADLEN_SegmentFile + SEGMENT_SIZE);
	int byteWrite = write(fd,Tmp,HEADLEN_SegmentFile + SEGMENT_SIZE);

	delete [] Tmp; //::free(Tmp);
	
	if(byteWrite != (HEADLEN_SegmentFile + SEGMENT_SIZE))
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "_newNextSeg() registered failed seg[%d]->fd[%d]:%s]"), idx, fd, segname);
		return false;
	}

	std::string key = PageManager::genShmkey(segname, 0);

	///add fd to _freeSegs
	LargeContentPage::PageDescriptor bufd;
	bufd.capacity = SEGMENT_SIZE;
	bufd.fd = fd;
	bufd.key = key;
	bufd.length = 0;
	bufd.stampAsOfOrigin = 0;
	bufd.flags = 0;

	_shmFileMap[fd] =  ShmFile(std::string(segname), bufd);

	_freeList.push_back(fd);
	_fdLists.push_back(fd);
	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "_newNextSeg() registered seg[%d]->fd[%d]:%s]"), idx, fd, segname);

	return true;
}

void ShmPages_Local::onFull(LargeContentPage::Ptr seg)
{
	int64 lStart = ZQ::common::now();

	//log(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "onFull() fd[%d] key[%s]flags[%d]"), seg->fd(),seg->key().c_str(),seg->flags());
	if(FLAG_ISSETn(seg->flags() , LargeContentPage::sfDirtyLeading))
	{
		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "onFull() key[%s] shm[%s(%d)] DirtyLeading data len[%d/%d] skip onFull, took %dms"),  seg->key().c_str(),fd2SegmentName(seg->fd()).c_str(), seg->fd(), seg->getLength(), seg->getCapacity(), (int)(ZQ::common::now() - lStart));
		return ;
	}

	ZQ::common::MutexGuard g(_lkShm);
	ShmFileMap::iterator itorShmFile = _shmFileMap.find(seg->fd());

	if (_shmFileMap.end() == itorShmFile)
	{
		// this must be a bug
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "onFull() key[%s] shm[%s(%d)] unknown fd, took %dms"), seg->key().c_str(), fd2SegmentName(seg->fd()).c_str(), seg->fd(), (int)(ZQ::common::now() - lStart));
		return;
	}

	itorShmFile->second._bufd.length = seg->getLength();
	itorShmFile->second._bufd.stampAsOfOrigin =  ZQ::common::now();
	itorShmFile->second.updateHeader();

	///read from diskCache, update timeStampRequest cacheLoadFd;
	SegmentDescriptor segD;
	segD.fd = seg->fd();
	segD.key = seg->key();
	segD.length = seg->getLength();
	segD.capacity = seg->getCapacity();
	segD.flags = seg->flags();
	segD.stampAsOfOrigin = seg->getstampAsOfOrigin();
	segD.pathName = fd2SegmentName(segD.fd);

	int cseqDCReq = -1;
	if (isConnected())
		cseqDCReq = call_flushSegment(segD, _cacheFlushTimeout);

	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "onFull() key[%s] shm[%s(%d)] data len[%d/%d] flags[%d] requested to cache: cseq(%d), took %dms"), 
		 seg->key().c_str(),fd2SegmentName(seg->fd()).c_str(), seg->fd(), seg->getLength(), seg->getCapacity(),segD.flags, cseqDCReq, (int)(ZQ::common::now() - lStart));
}

void ShmPages_Local::updateSegment(LargeContentPage::Ptr seg)
{
/*
	if(seg->isDirtyLeading())
	{
		ZQ::common::MutexGuard g(_lkShm);
		ShmFileMap::iterator itorShmFile = _shmFileMap.find(seg->fd());

		if (_shmFileMap.end() == itorShmFile )
		{
			// this must be a bug
			TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "updateSegment() unknown fd[%d] took %dms"), seg->fd(), (int)(ZQ::common::now() - lStart));
			return;
		}

		FLAG_SETn(itorShmFile->second._bufd.flags, LargeContentPage::sfDirtyLeading);
	}
*/
}

void ShmPages_Local::OnSegmentLoaded(SegmentDescriptor& seg, int err)
{
	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnSegmentLoaded() key[%s] shm[%s(%d)] len[%d/%d] flags[%d] error[%d]"),
		seg.key.c_str(),  seg.pathName.c_str(), seg.fd, seg.length, seg.capacity, seg.flags, err);

	ZQ::common::MutexGuard g(_lkShm);

	KeyToFdIndex::iterator itUsed = _usedIdxKeyToFd.find(seg.key);
	if(itUsed == _usedIdxKeyToFd.end())
		return;

	UsedInfo& usedInfo = itUsed->second;
	if(!usedInfo.bReadingFromDiskCache || usedInfo.cacheLoadFd <= 0 )
		return;

	ShmFileMap::iterator itorShmFile = _shmFileMap.find(usedInfo.cacheLoadFd);
	if (_shmFileMap.end() == itorShmFile)
		return;

	if(_shmFileMap[usedInfo.cacheLoadFd]._bufd.key != seg.key)
		return;

	usedInfo.timeStampResponse = ZQ::common::now();
	usedInfo.bReadingFromDiskCache = false;

	int costTime = (int)(usedInfo.timeStampResponse - usedInfo.timeStampRequest);

	int freefd = usedInfo.cacheLoadFd;
	//read from dishcache success
	if(err == ZQ::sloop::CPMIMessage::cpmeOK)
	{
		_shmFileMap[usedInfo.cacheLoadFd]._bufd.length = seg.length;
		_shmFileMap[usedInfo.cacheLoadFd]._bufd.capacity  = seg.capacity;
		_shmFileMap[usedInfo.cacheLoadFd]._bufd.stampAsOfOrigin  = seg.stampAsOfOrigin;
		_shmFileMap[usedInfo.cacheLoadFd]._bufd.flags =  seg.flags;
		usedInfo.usedFds.push_back(usedInfo.cacheLoadFd);

		_count(DISKCACHE_HIT);
		_recycleFd(freefd, "OnSegmentLoaded() successful");

		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnSegmentLoaded() key[%s] shm[%s(%d)] response fd(%d), loaded from DiskCache, updated data len[%d/%d], took %dms"),
				seg.key.c_str(), seg.pathName.c_str(), usedInfo.cacheLoadFd, seg.fd, seg.length, seg.capacity, costTime);

		usedInfo.cacheLoadFd = -1;
	}
	else //read from dishcache failed, add fd to _freeList, sign the key is not read from diskcache 
	{
		TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "OnSegmentLoaded() key[%s] shm[%s(%d)] response fd(%d), read data from DiskCache caught error(%d), took %dms"),
			seg.key.c_str(), seg.pathName.c_str(), usedInfo.cacheLoadFd, seg.fd, err, costTime);

		_recycleFd(freefd, "OnSegmentLoaded() failed");
		usedInfo.cacheLoadFd = -1;
	}
}

void ShmPages_Local::OnSegmentFlushed(SegmentDescriptor& seg, int err)
{
	if(err == ZQ::sloop::CPMIMessage::cpmeOK)
	{
		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnSegmentFlushed() key[%s] shm[%s]  flush data to DiskCache successfully, data len[%d/%d],flags[%d]"),
			seg.key.c_str(), seg.pathName.c_str(), seg.length, seg.capacity,seg.flags);
	}
	else 
	{
		TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "OnSegmentFlushed() key[%s] shm[%s] flush data to DiskCache caught error[%d]"),
			seg.key.c_str(), seg.pathName.c_str(), err);
	}
}

#if 0
///implement LIPCClient eloop callback
void ShmPages_Local::OnConnected(ZQ::eloop::Handle::ElpeError status)
{
	/*if (status != ZQ::eloop::Handle::elpeSuccess)
	{
		onError(status, ZQ::eloop::Handle::errDesc(status));
		return;
	}*/
	LIPCClient::OnConnected(status);

	if( status != ZQ::eloop::Handle::elpeSuccess)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "OnConnected() DiskCache[%s] connected recv err[%d]"), _diskCacheServerPipeName.c_str(), status);
		return;
	}
	
	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnConnected() DiskCache[%s] connected"), _diskCacheServerPipeName.c_str());
	_connOK = true;
	_stampConnectingDiskCache = ZQ::common::now();
	//read_start();
}

// errors from the LIPC to DiskCache
void ShmPages_Local::onError(int error, const char* desc)
{	
	switch(error)
	{
	//following errors may be the LIPC connect broken, we should reconnect
	case ZQ::eloop::Handle::elpuECONNREFUSED:
	case ZQ::eloop::Handle::elpuECONNRESET:
	case ZQ::eloop::Handle::elpeBadFd:
	case -111:
	case -4095:
		LIPCClient::onError(error, desc);
		TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "CPMI-to-diskcache[%s] broken, will reconncet, err(%d) %s"), _diskCacheServerPipeName.c_str(), error, desc);
		
		_connOK = false;
		_stampConnectingDiskCache = ZQ::common::now();
		break;

	default:
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "CPMI-to-diskcache[%s] onError(%d) %s"), _diskCacheServerPipeName.c_str(), error, desc);
	}
}

//void ShmPages_Local::OnClose()
//{
//	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnClose() close diskcache pipe[%s]"), _diskCacheServerPipeName.c_str());
//}
#endif // 0

void ShmPages_Local::stepDo(bool byHeartbeat)
{
//	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "OnTimer()"));
    
	CPMICLIENT(DiskCache)::stepDo(byHeartbeat);

	//if( !_bDisabledDiskCache && !_connOK  && ((ZQ::common::now() - _stampConnectingDiskCache) > RECONNECT_INTERVAL_DEFAULT))
	//{
	//	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "stepDo() connecting diskcache pipe[%s]"), _diskCacheServerPipeName.c_str());
	//	ZQ::eloop::LIPCClient::connect((char*)_diskCacheServerPipeName.c_str());
	//	_stampConnectingDiskCache = ZQ::common::now();
	//}

	if( (ZQ::common::now() - _stampClearUsed) <=  CLEARUSED_INTERVAL )
		return;

	_stampClearUsed = ZQ::common::now();

	ZQ::common::MutexGuard g(_lkShm);
	std::vector <std::string> clearKeys;

	for(KeyToFdIndex::iterator itUsed = _usedIdxKeyToFd.begin(); itUsed != _usedIdxKeyToFd.end(); itUsed++)
	{
		UsedInfo& usedinfo = itUsed->second;
		if(usedinfo.bReadingFromDiskCache && usedinfo.timeStampResponse <= 0 && usedinfo.timeStampRequest > 0 && usedinfo.cacheLoadFd > 0)
		{
			int timeout = ZQ::common::now() - usedinfo.timeStampRequest;
			if (timeout < (_cacheLoadTimeout * 5))
			{
				TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnTimer() key[%s]fds[%d] unexpired shm[%s(%d)] bReadingFromDiskCache[%s, %lld-%lld]"), 
						(itUsed->first).c_str(), usedinfo.usedFds.size(), fd2SegmentName(usedinfo.cacheLoadFd).c_str(), usedinfo.cacheLoadFd, usedinfo.bReadingFromDiskCache? "YES" :"NO", usedinfo.timeStampRequest, usedinfo.timeStampResponse);

				continue;
			}

			TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnTimer() key[%s] timeout for shm[%s(%d)] add to freelist"), (itUsed->first).c_str(), fd2SegmentName(usedinfo.cacheLoadFd).c_str(), usedinfo.cacheLoadFd);

			//_freeList.push_back(usedinfo.cacheLoadFd);

			_recycleFd(usedinfo.cacheLoadFd, "onTimer()");

			if(usedinfo.usedFds.empty())
				clearKeys.push_back(itUsed->first);
			else{
				usedinfo.bReadingFromDiskCache = false;
				usedinfo.cacheLoadFd = -1;
				usedinfo.timeStampRequest = 0;
				usedinfo.timeStampResponse = 0;
			}

			continue;
		}

		if(usedinfo.cacheLoadFd > 0 && usedinfo.bReadingFromDiskCache)
		{
			TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnTimer() key[%s]fds[%d] shm[%s(%d)] bReadingFromDiskCache[%s, %lld-%lld]"), 
				(itUsed->first).c_str(), usedinfo.usedFds.size(), fd2SegmentName(usedinfo.cacheLoadFd).c_str(), usedinfo.cacheLoadFd, usedinfo.bReadingFromDiskCache? "YES" :"NO", usedinfo.timeStampRequest, usedinfo.timeStampResponse);
		}
	}

	for (size_t i = 0; i < clearKeys.size(); i++)
	{
		size_t  before = _usedIdxKeyToFd.size();
		_usedIdxKeyToFd.erase( clearKeys[i]);

		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "OnTimer() remove key[%s] from usedIdxKeyToFd map [%d==>%d]"), clearKeys[i].c_str(), before, _usedIdxKeyToFd.size());
	}
}

#define MAXSTATISTICSCOUNT (1<<29)
void ShmPages_Local::_count(StatisticsType type)
{
	switch(type)
	{
	case MEMORY_HIT:
		_shmStatistics.cHitMem++;
		break;

	case DISKCACHE_HIT:
		_shmStatistics.cHitDisk++;
		break;

	case MISSED_HIT:
		_shmStatistics.cMissed++;
		break;

	default:
		break;
	}

	if(_shmStatistics.cHitMem > MAXSTATISTICSCOUNT || _shmStatistics.cHitDisk > MAXSTATISTICSCOUNT || _shmStatistics.cMissed > MAXSTATISTICSCOUNT)
	{
         _shmStatistics.cHitMem = 0;
		 _shmStatistics.cHitDisk = 0;
		 _shmStatistics.cMissed = 0;
	}	
}

void ShmPages_Local::resetStatistics()
{
	ZQ::common::MutexGuard g(_lkShm);
	_shmStatistics.cHitDisk = 0;
	_shmStatistics.cHitMem = 0;
	_shmStatistics.cMissed = 0;
}

bool lesser (int elem1,  int elem2 )
{
	return elem1 < elem2;
}

static std::string Ints2String(const std::vector<int>& fds)
{
	std::string  strFds;
	for(int i = 0; i < fds.size(); i++)
	{
		char buf[64];
		memset(buf, 0, sizeof(buf));
		itoa(fds[i], buf, 10);
		strFds +=  std::string(buf) + std::string(";");
	}
	return strFds;
}

ShmPages_Local::Statistics ShmPages_Local::getStatistics()
{
	ZQ::common::MutexGuard g(_lkShm);

	_shmStatistics.free = _freeList.size();
	_shmStatistics.total = _maxSegs;
	_shmStatistics.used = 0;
	_shmStatistics.diskcache = 0;

    FdSet tempList;

	KeyToFdIndex::iterator itorKey = _usedIdxKeyToFd.begin();
	for( ; _usedIdxKeyToFd.end() != itorKey; itorKey++)
	{
		//_shmStatistics.used += itorKey->second.usedFds.size();
		if(itorKey->second.bReadingFromDiskCache)
		{
			_shmStatistics.diskcache++;
			tempList.push_back(itorKey->second.cacheLoadFd);
		}
	}

	std::string freelist;
	for(ShmFileMap::iterator itor = _shmFileMap.begin(); itor != _shmFileMap.end(); ++itor)
	{
		if(itor->second.getHatched() > 0)
		{
			_shmStatistics.used++;
			tempList.push_back(itor->second._bufd.fd);
		}
	/*	else
		    _shmStatistics.diskcache++;

		FreeList::iterator itorFree = std::find(_freeList.begin(), _freeList.end(), itor->second._bufd.fd);

		if(itorFree == _freeList.end())
		{
			char buf[10];
			memset(buf,0,sizeof(buf));
			itoa(itor->second._bufd.fd, buf, 10);
			freelist+= buf + std::string("; ");
		}*/
	}

	int segCount = _shmStatistics.diskcache + _shmStatistics.used +  _shmStatistics.free;

	if (segCount < _maxSegs)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "getStatistics() potential shmseg leak detected: %d free + %d DC + %d used < max %d"), _shmStatistics.free,_shmStatistics.diskcache, _shmStatistics.used, _maxSegs);

		tempList.insert(tempList.end(), _freeList.begin(), _freeList.end());
		std::sort(tempList.begin(), tempList.end(), lesser);
		std::sort(_fdLists.begin(), _fdLists.end(), lesser);

		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "getStatistics() fdlist[%s] "), Ints2String(tempList).c_str());

		FdSet result;
		result.resize(_maxSegs);

		FdSet::iterator retEndPos = set_symmetric_difference(tempList.begin(), tempList.end(), _fdLists.begin(), _fdLists.end(), result.begin());
		result.resize(retEndPos - result.begin()); 

		TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "getStatistics() missed %d fds, fdlist[%s] "), result.size(), Ints2String(result).c_str());
         
		for (int i = 0; i < result.size(); i++)
		{
			ShmFileMap::iterator itorShmFile = _shmFileMap.find(result[i]);
			if (_shmFileMap.end() == itorShmFile)
			{
				TRACE(ZQ::common::Log::L_WARNING, CLOGFMT(ShmSegments, "getStatistics() fd[%d] unknown"), result[i]);
				continue;
			}

			TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "getStatistics() missed shm[%s(%d)] getHatched[%d] key[%s] len[%u/%u]"),
				fd2SegmentName(itorShmFile->first).c_str(), itorShmFile->first, (int)itorShmFile->second.getHatched(), itorShmFile->second._bufd.key.c_str(), itorShmFile->second._bufd.length, itorShmFile->second._bufd.capacity);
		}
	}

//	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "getStatistics() usedfd[%s] ,_shmFileMap[%d],_freeList[%d]"),freelist.c_str(),_shmFileMap.size(),_freeList.size());

	return _shmStatistics;
}

size_t ShmPages_Local::freeSize()
{
	ZQ::common::MutexGuard g(_lkShm);
	return _freeList.size();
}

size_t ShmPages_Local::_recycleFd(int freeFd, const char* reason)
{
	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(ShmSegments, "_recycleFd() shm[%s(%d)] per %s"), fd2SegmentName(freeFd).c_str(), freeFd, reason);

	_freeList.push_back(freeFd);
	return _freeList.size();
}

/*
void ShmPages_Local::rpcAllocate(const  ZQ::eloop::LIPCRequest::Ptr& req,  ZQ::eloop::LIPCResponse::Ptr& resp)
{
	//	TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(LocalShmSegmentManager, "rpcAllocate() enter"));

	ZQ::LIPC::Arbitrary resp;
	resp[JSON_RPC_ID] = req.isMember(JSON_RPC_ID) ? req[JSON_RPC_ID] : ZQ::LIPC::Arbitrary::null;
	resp[JSON_RPC_PROTO] = JSON_RPC_PROTO_VERSION;

	std::string uriOrigin = "", key = "";
	int64 offsetOrigin = -1;
	ZQ::LIPC::Arbitrary params = req[JSON_RPC_PARAMS];

	if(JSON_HAS(params,"uriOrigin"))
		uriOrigin = params["uriOrigin"].asString();

	if(JSON_HAS(params,"offsetOrigin"))
		offsetOrigin = params["offsetOrigin"].asInt64();

	if(params.isMember("key"))
		key = params["key"].asString();

	int rpcId = resp[JSON_RPC_ID].asInt();
	Json::FastWriter writer;
	std::string strParams = writer.write(params);
	TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "rpcAllocate() id[%d] request params[%s]"), rpcId, strParams.c_str());

	LargeContentPage::Ptr streamSegPtr = NULL;
	int fd = -1;
	if((!uriOrigin.empty() && offsetOrigin >=0  && !key.empty()))
	{
		streamSegPtr = allocate(uriOrigin, offsetOrigin);
	}

	if(streamSegPtr != NULL)
	{
		ZQ::LIPC::Arbitrary resParams;
		resParams["uriOrigin"] =  uriOrigin;
		resParams["offsetOrigin"] = (int64)streamSegPtr->getOffsetOfOrigin();
		resParams["fd"] = fd = streamSegPtr->fd();
		resParams["key"] = streamSegPtr->key();
		resParams["capacity"] = streamSegPtr->getCapacity();
		resParams["length"] =  streamSegPtr->getLength();

		resp[JSON_RPC_RESULT] = resParams;

		Json::FastWriter writer;
		std::string strrespParams = writer.write(resParams);

		TRACE(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "rpcAllocate() id[%d] successfull, response params[%s]"), rpcId,  strrespParams.c_str());
	}
	else
	{
		ZQ::LIPC::Arbitrary error;
		error[JSON_RPC_ERROR_CODE] = ZQ::LIPC::Handler::INVALID_PARAMS;
		error[JSON_RPC_ERROR_MESSAGE] = ZQ::LIPC::Handler::errDesc(ZQ::LIPC::Service::INVALID_PARAMS);
		resp[JSON_RPC_ERROR] = error;

		Json::FastWriter writer;
		std::string strParams = writer.write(resp[JSON_RPC_ERROR]);

		log(ZQ::common::Log::L_ERROR, CLOGFMT(ShmSegments, "rpcAllocate() id[%d] failure, response params[%s]"), rpcId,  strParams.c_str());

		fd = -1;
	}
	conn.sendfd(resp, fd);
	//	log(ZQ::common::Log::L_DEBUG, CLOGFMT(LocalShmSegmentManager, "rpcAllocate() leave"));
}

void ShmPages_Local::rpcFree(const  ZQ::eloop::LIPCRequest::Ptr& req,  ZQ::eloop::LIPCResponse::Ptr& resp)
{
	//	log(ZQ::common::Log::L_DEBUG, CLOGFMT(LocalShmSegmentManager, "rpcFree() enter"));
	ZQ::LIPC::Arbitrary resp;
	resp[JSON_RPC_ID] = req.isMember(JSON_RPC_ID) ? req[JSON_RPC_ID] : ZQ::LIPC::Arbitrary::null;
	resp[JSON_RPC_PROTO] = JSON_RPC_PROTO_VERSION;

	std::string key = "";

	ZQ::LIPC::Arbitrary params = req[JSON_RPC_PARAMS];

	if(JSON_HAS(params,"key"))
		key = params["key"].asString();

	int rpcId = resp[JSON_RPC_ID].asInt();
	Json::FastWriter writer;
	std::string strParams = writer.write(params);
	log(ZQ::common::Log::L_INFO, CLOGFMT(ShmSegments, "rpcFree() id[%d] params[%s]"), rpcId,  strParams.c_str());
}
*/

}}//end namesapce  ZQ::memory
