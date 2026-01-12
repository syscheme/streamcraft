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
// Ident : $Id: StreamSegment.cpp $
// Branch: $Name:  $
// Author: Hui Shao
// Desc  : Define stream segment
//
// Revision History: 
// ---------------------------------------------------------------------------
// $Log: /ZQProjs/Common/Exception.h $
// ===========================================================================

#include "StreamSegment.h"
#include "TimeUtil.h"

#ifndef ZQ_OS_MSWIN
extern "C" {
#include <sys/sendfile.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
}
#endif // ZQ_OS

namespace syscheme {
namespace StreamCraft {
	
#define __N2S2__(x) #x
#define __N2S__(x) __N2S2__(x)

// -----------------------------
// class StreamSegment
// -----------------------------
StreamSegment::StreamSegment(SegmentManager& segm, BufDescriptor& bufd, const std::string& uriOrigin, int64 offsetOrigin, int HeadLength)
:_segm(segm), _bufd(bufd),_headLength(HeadLength)
{
	_offsetOfOrigin = SEG_IDX_OF_OFFSET(offsetOrigin);

/*	char str[SEGMENT_KEYLEN + 5];
	snprintf(str, sizeof(str)-2, ":%0" __N2S__(SEGMENT_KEYLEN) "X", _offsetOfOrigin);
	_bufd.key = uriOrigin + ":" + str;
*/

	if(_bufd.key.empty())
		_bufd.key =  SegmentManager::genShmkey(uriOrigin, _offsetOfOrigin);

	_offsetOfOrigin = SEG_STARTOFFSET(_offsetOfOrigin);
}

StreamSegment::~StreamSegment()
{
	_segm.free(_bufd);
}

void StreamSegment::resetAttr(const std::string& uriOrigin, int64 offsetOrigin)
{
	_offsetOfOrigin = SEG_IDX_OF_OFFSET(offsetOrigin);
	/*	char str[SEGMENT_KEYLEN + 5];
	snprintf(str, sizeof(str)-2, ":%0" __N2S__(SEGMENT_KEYLEN) "X", _offsetOfOrigin);
	_bufd.key = uriOrigin + ":" + str;
	*/

	_bufd.key = SegmentManager::genShmkey(uriOrigin, _offsetOfOrigin);
	_offsetOfOrigin = SEG_STARTOFFSET(_offsetOfOrigin);

	_bufd.length = 0;

	if(_bufd.ptr)
		memset(_bufd.ptr,'\0',_bufd.maxSize + _headLength);
}

uint8* StreamSegment::ptr()
{
	if (NULL != _bufd.ptr)
		return _bufd.ptr;

	if (_bufd.fd > 0)
	{
		// TODO: mmap the fd to address ptr
	}

	return _bufd.ptr;
}

uint StreamSegment::addLength(uint len)
{
	uint originLen = _bufd.length;
	_bufd.length += len;
	if(_bufd.length > _bufd.maxSize)
		_bufd.length = _bufd.maxSize;

	return (_bufd.length - originLen);
}

void StreamSegment::setReadOnly(bool readonly) 
{ 
	if (readonly) 
		FLAG_SETn(_bufd.flags, sfReadOnly); 
	else FLAG_CLRn(_bufd.flags, sfReadOnly); 
}

// fill data from the source to this segment by fd
//@param fdSource, the fd of the source
//@param length, the number of bytes to read from the source and fill into the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start fill
//@return the number of bytes has successfully filled into the segment
int StreamSegment::fill(int fdSource, uint length, int startOffset)
{
	// TODO:
	return 0;
}

// fill data from the source to this segment by memory address
//@param source, the memory address of the source
//@param length, the number of bytes to read from the source and fill into the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start fill
//@return the number of bytes has successfully filled into the segment
int StreamSegment::fill(uint8* source, uint length, int startOffset)
{
	uint8* target = ptr(), *t = target;
	if (NULL == target || NULL == source)
	{
		_segm._lw(ZQ::common::Log::L_ERROR, CLOGFMT(StreamSegment, "fill() failed: source[%p] target[%p]"), source, target);
		return 0;
	}

	if (startOffset > 0)
		t += startOffset;

	uint len = _bufd.maxSize - startOffset;
	if (len > length)
		len = length;

	_bufd.length += len;
	memcpy(t, source, len);
	return len;
}

// output data from the segement to a target by fd
//@param fdTarget, the fd of the target
//@param length, the number of bytes to read from the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start flushing
//@return the number of bytes has successfully filled into the segment
int StreamSegment::flush(int fdTarget, uint length, int startOffset)
{
	// TODO:
	return 0;
}

// output data from the segement to a target by memory address
//@param target, the memory address of the target
//@param length, the number of bytes to read from the source and fill into the segment
//@param startOffset, if >=0, to specify the offset within the segment where to start fill
//@return the number of bytes has successfully filled into the segment
int StreamSegment::flush(uint8* target, uint length, int startOffset)
{
	uint8* source = ptr(), *s = source;
	if (NULL == target || NULL == source)
	{
		_segm._lw(ZQ::common::Log::L_ERROR, CLOGFMT(StreamSegment, "flush() failed: source[%p] target[%p]"), source, target);
		return 0;
	}

	if (startOffset > 0)
		s += startOffset;

	uint len = _bufd.maxSize - startOffset;
	if (len > length)
		len = length;

	memcpy(target, s, len);
	return len;
}

// -----------------------------
// class SegmentManager
// -----------------------------
#define TRACE(LEVEL, ...)      WRAPPEDLOG(_lw, LEVEL, __VA_ARGS__)

SegmentManager::SegmentManager(ZQ::common::Log& log, ZQ::eloop::Loop& loop,uint segPoolInit, uint segPoolMax) 
:_lw(log),_segPoolMax(segPoolMax),_verboseFlags(1)
{
	for(uint i=0; i<segPoolInit; i++)
	{
		StreamSegment::BufDescriptor bufd;
		bufd.maxSize = SEGMENT_SIZE;
		bufd.ptr = new uint8[SEGMENT_SIZE];
		if (NULL == bufd.ptr)
		{
			TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(StreamManager, "new memory error."));
			continue;
		}

		_segIdle.push_back(bufd);
		_segPool.push_back(bufd);
	}
}

std::string SegmentManager::genShmkey(const std::string& uriOrigin, int64 offsetOrigin)
{
	//char str[32];
	//memset(str, 0, sizeof(str));
	//snprintf(str, sizeof(str)-2, "%lld", offsetOrigin);

	char str[SEGMENT_KEYLEN + 5] ="";
	snprintf(str, sizeof(str)-2, ":%llX", offsetOrigin);
	std::string key = uriOrigin + ":" + str;
	return key;   
}

// allocate a segment with key as the purpsoe
StreamSegment::Ptr SegmentManager::allocate(const std::string& uriOrigin, int64 offsetOrigin)
{
	if (_segIdle.empty())
	{
		if (_segPool.size() < _segPoolMax)
		{
			StreamSegment::BufDescriptor bufd;
			bufd.maxSize = SEGMENT_SIZE;
			bufd.ptr = new uint8[SEGMENT_SIZE];
			if (NULL == bufd.ptr)
			{
				TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(StreamManager, "new memory error."));
				return NULL;
			}
			_segIdle.push_back(bufd);
			_segPool.push_back(bufd);
		}
		else
		{
			TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(StreamManager, "Memory pool full. pool size =%d"),_segPool.size());
			return NULL;
		}
	}
	
	StreamSegment::BufDescriptor bufd = _segIdle.front();
	_segIdle.pop_front();

	StreamSegment::Ptr seg = hatchSegObj<StreamSegment>(bufd);
	if (seg == NULL)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(StreamManager, "new StreamSegment error."));
		return NULL;
	}

	seg->resetAttr(uriOrigin,offsetOrigin);
	return seg;
}

static const uint8 NULL_PACKET[TSPACKET_LENTH] = {
	0x47, 0x1f, 0xff, 0x10, 0x43, 0x54, 0x46, 0x6c, 0x69, 0x62,
	0x00, 0x33, 0x03, 0x4c, 0x4e, 0x58, 0x00, 0x4e, 0x6f, 0x76,
	0x20, 0x31, 0x30, 0x20, 0x32, 0x30, 0x31, 0x36, 0x20, 0x32,
	0x32, 0x3a, 0x32, 0x33, 0x3a, 0x34, 0x34, 0x00, 0x4a, 0x75,
	0x6e, 0x20, 0x20, 0x31, 0x33, 0x20, 0x32, 0x30, 0x31, 0x37,
	0x20, 0x30, 0x39, 0x3a, 0x34, 0x37, 0x3a, 0x32, 0x30, 0x00,
	0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4d, 0x3f,
	0x4f, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
	0x01, 0x00, 0x52, 0x18, 0x02, 0x2e, 0x46, 0x52, 0x31, 0x00,
	0x30, 0x32, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x51,
	0xe0, 0xc6, 0x0e, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
};

StreamSegment::Ptr SegmentManager::newNullSegment(const std::string& uriOrigin, int64 offsetOrigin)
{
	// build up the static __segNullPackets, and let the readonly seg to map to it
	static std::string __segNullPackets;
	if (__segNullPackets.empty())
	{
		for (int i=0; i< (SEGMENT_SIZE /TSPACKET_LENTH)+10; i++)
			__segNullPackets.append((const char*)NULL_PACKET, TSPACKET_LENTH);
	}

	StreamSegment::BufDescriptor bufd;
	bufd.maxSize = SEGMENT_SIZE;
	bufd.length  = SEGMENT_SIZE;
	FLAG_SETn(bufd.flags, StreamSegment::sfReadOnly);
	bufd.ptr = (uint8*) (__segNullPackets.c_str() + (offsetOrigin %TSPACKET_LENTH));

	StreamSegment::Ptr seg = hatchSegObj<StreamSegment>(bufd);
	if (!seg)
	{
		TRACE(ZQ::common::Log::L_ERROR, CLOGFMT(PauseWin, "newNullSegment() uri[%s] %lld failed"), uriOrigin.c_str(), offsetOrigin);
		return NULL;
	}

	seg->resetAttr(uriOrigin, offsetOrigin);
	return seg;
}

void SegmentManager::freeNullSegment(StreamSegment::Ptr seg)
{
	// do nothing
}

//StreamSegment::Ptr SegmentManager::allocateEmptyPacket(const std::string& uriOrigin, int64 offsetOrigin)
//{
//	StreamSegment::BufDescriptor bufd;
//	bufd.maxSize = SEGMENT_SIZE;
//	bufd.ptr = new uint8[SEGMENT_SIZE];
//	if (NULL == bufd.ptr)
//	{
//		_log(ZQ::common::Log::L_ERROR, CLOGFMT(SegmentManager, "new memory error."));
//		return NULL;
//	}
//
//	StreamSegment::Ptr seg = hatchSegObj<StreamSegment>(bufd);
//	if (seg == NULL)
//	{
//		_log(ZQ::common::Log::L_ERROR, CLOGFMT(SegmentManager, "new StreamSegment error."));
//		return NULL;
//	}
//
//	seg->resetAttr(uriOrigin, offsetOrigin);
//	return seg;
//}
//
//void SegmentManager::freeEmptyPacket(StreamSegment::Ptr seg)
//{
//	if (NULL != seg->_bufd.ptr)
//		delete[] seg->_bufd.ptr;
//
//	seg->_bufd.ptr = NULL;
//	seg->_bufd.fd = INVALID_FD;
//}
//
void SegmentManager::free(StreamSegment::Ptr seg)
{
	if (NULL == seg || NULL == seg->_bufd.ptr)
		return;

	return free(seg->_bufd);
}

void SegmentManager::free(StreamSegment::BufDescriptor bufd)
{
	_segIdle.push_back(bufd);
}

void SegmentManager::clear()
{
	while(!_segPool.empty())
	{
		StreamSegment::BufDescriptor bufd = _segPool.front();
		if (bufd.ptr) delete[] bufd.ptr;
		bufd.ptr = NULL;
		bufd.fd = INVALID_FD;
		_segPool.pop_front();
		TRACE(ZQ::common::Log::L_DEBUG, CLOGFMT(StreamSegment, "free segment"));
	}

	while(!_segIdle.empty())
		_segIdle.pop_front();
}

void SegmentManager::onFull(StreamSegment::Ptr seg)
{
}

void SegmentManager::updateSegment(StreamSegment::Ptr seg)
{
}

// -----------------------------
// class FDWrapper
// -----------------------------
static FDWrapper::Map _preOpenedForLoad; // map from pathname to fd
static ZQ::common::Mutex _lkOpenedForLoad; // map from pathname to fd

FDWrapper::Ptr FDWrapper::openForLoad(const std::string& pathname, ZQ::common::Log *pLog)
{
	{
		ZQ::common::MutexGuard g(_lkOpenedForLoad);
		FDWrapper::Ptr ret = NULL;
		if (_preOpenedForLoad.find(pathname) != _preOpenedForLoad.end())
		{
			ret = _preOpenedForLoad[pathname];
			_preOpenedForLoad.erase(pathname);

			if(ret && pLog)
				(*pLog)(ZQ::common::Log::L_DEBUG, CLOGFMT(FDWrapper, "openForLoad() pathName[%s] hits fd[%d]"), pathname.c_str(), ret->getFD());

			return ret;
		}
	}

	return new FDWrapper(open(pathname.c_str(), O_RDWR));
}

bool FDWrapper::preOpenForLoad(const std::string& pathname, ZQ::common::Log *pLog)
{
	FDWrapper::Ptr pFD =new FDWrapper(open(pathname.c_str(), O_RDWR));
	if(!pFD.get() && !pFD->isVaildFD())
		return false;

	ZQ::common::MutexGuard g(_lkOpenedForLoad);
	_preOpenedForLoad[pathname] = pFD;
	if(pLog)
	{
		(*pLog)(ZQ::common::Log::L_DEBUG, CLOGFMT(FDWrapper, "preOpenForLoad() pathName[%s] store fd[%d]"), pathname.c_str(), pFD->getFD());
	}
	return true;
}

// -----------------------------
// class SegmentFile
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
#define HEADLEN_SegmentFile (512)
#define URL_MAX_LENGTH      (300)

#define SegmentFile_MagicNumber    (0x0131C9A5)
#define SegmentFile_CurrentVersion (0x0100)

SegmentFile::IOError SegmentFile::bufDescToHeader(const StreamSegment::BufDescriptor& segBufd, Header& fheader)
{
	memset(&fheader, 0x00, sizeof(fheader));
	fheader.data.signature       = SegmentFile_MagicNumber;
	fheader.data.version         = SegmentFile_CurrentVersion;

	fheader.data.stampAsOfOrigin = segBufd.stampAsOfOrigin;
	fheader.data.flags           = segBufd.flags;
	fheader.data.length          = segBufd.length;
	int maxlen = (int)(((uint8*)&fheader) + sizeof(fheader) - ((uint8*)&fheader.data.urlKey) -2);
	strncpy(fheader.data.urlKey, segBufd.key.c_str(), maxlen);

	// fixup
	if (fheader.data.stampAsOfOrigin <=0)
		fheader.data.stampAsOfOrigin = ZQ::common::now();

	return eOK;
}

SegmentFile::IOError SegmentFile::headerToBufDesc(Header& fheader, StreamSegment::BufDescriptor& segBufd)
{
	if ((SegmentFile_MagicNumber != fheader.data.signature) || (SegmentFile_CurrentVersion != fheader.data.version))
		return eBadHeader;

	// validate the header data

	segBufd.stampAsOfOrigin = fheader.data.stampAsOfOrigin;
	segBufd.flags           = fheader.data.flags;
	segBufd.length          = fheader.data.length;
	int maxlen = (int)(((uint8*)&fheader) + sizeof(fheader) - ((uint8*)&fheader.data.urlKey) -2);
	// fheader.data.urlKey[maxlen] ='\0';
	segBufd.key = fheader.data.urlKey;

	return eOK;
}

#ifdef ZQ_OS_MSWIN
SegmentFile::IOError SegmentFile::save(const std::string& pathname, StreamSegment::BufDescriptor& segBufd)
{
	HANDLE hFile = ::CreateFile(pathname.c_str(), GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
	if (INVALID_HANDLE_VALUE == hFile)
		return eIOError;

	// prepare the header
	Header fheader;
	bufDescToHeader(segBufd, fheader);

	IOError err= eIOError;
	do {
		DWORD lenWritten =0;
		if (FALSE == ::WriteFile(hFile, &fheader, sizeof(fheader), &lenWritten, NULL) || lenWritten < sizeof(fheader))
			break;

		if (::WriteFile(hFile, segBufd.ptr, fheader.data.length, &lenWritten, NULL) || lenWritten < fheader.data.length)
			break;

		err= eOK;

	} while(0);

	::CloseHandle(hFile);
	return err;
}

SegmentFile::IOError SegmentFile::load(const std::string& pathname, StreamSegment::BufDescriptor& segBufd,  ZQ::common::Log *pLog)
{
	HANDLE hFile = ::CreateFile(pathname.c_str(), GENERIC_READ, FILE_SHARE_READ, 0, OPEN_EXISTING, 0, 0);
	if (INVALID_HANDLE_VALUE == hFile)
		return eNotFound;

	Header fheader;
	DWORD lenWritten =0;
	IOError err= eIOError;
	do {
		if (FALSE == ::ReadFile(hFile, &fheader, sizeof(fheader), &lenWritten, NULL) || lenWritten < sizeof(fheader))
		{
			err = eNoHeader;
			break;
		}
		err = headerToBufDesc(fheader, segBufd);
		if (eOK != err)
			break;

		err = eBadData;
		if (!segBufd.ptr || fheader.data.length > segBufd.maxSize)
			break;

		if (FALSE == ::ReadFile(hFile, segBufd.ptr, fheader.data.length, &lenWritten, NULL) || lenWritten < fheader.data.length)
			err = eBadData;

		err= eOK;

	} while(0);

	::CloseHandle(hFile);
	return err;
}

#else

void testReadFile(int fd, const std::string& key)
{
#define OPEN_FLAG (O_RDWR|O_CREAT)
#define OPEN_MODE 00777

	uint8 * ptr = (uint8 *)::mmap(NULL, SEGMENT_SIZE + HEADLEN_SegmentFile, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);

	if (ptr == MAP_FAILED)
	{
		return;
	}

	uint8* target = new uint8[HEADLEN_SegmentFile + SEGMENT_SIZE];

	memcpy(target, ptr, HEADLEN_SegmentFile + SEGMENT_SIZE);
	
//	printf("read key[%s]\n",  key.c_str());
	if(target != NULL)
	{
	/*	for(int len = 0; len < HEADLEN_SegmentFile + SEGMENT_SIZE; len ++)
		{
			printf("%c", target[len]);
		}*/

		printf("	front 640 byte: ");
		for(int len = 0; len < 640; len ++)
		{
			printf("%c", target[len]);
		}
	    printf("\n	end 128 byte: ");
		for(int len = HEADLEN_SegmentFile + SEGMENT_SIZE - 128; len < (HEADLEN_SegmentFile + SEGMENT_SIZE); len ++)
		{
			printf("%c", target[len]);
		}
		printf("\n");
		//printf("\n testReadFile: read %d byte\n\n", len);

		delete target;
	}

	::munmap(ptr, SEGMENT_SIZE + HEADLEN_SegmentFile);
}
SegmentFile::IOError SegmentFile::save(int fdFile, const StreamSegment::BufDescriptor& segBufd)
{
//	printf("save()key[%s]\n\n",  segBufd.key.c_str());

	if (fdFile <=0)
		return eIOError;

	// prepare the header
	Header fheader;
	bufDescToHeader(segBufd, fheader);

	// write the file header
	if (write(fdFile, &fheader, sizeof(fheader)) < sizeof(fheader))
		return eIOError;

	// write the data
	if (segBufd.fd >0)
	{
//		printf("save() shmSegment file. key[%s]\n\n",  segBufd.key.c_str());

		if(lseek(segBufd.fd, sizeof(fheader), SEEK_SET) == -1)
			return eIOError;

		if (::sendfile(fdFile, segBufd.fd, NULL, fheader.data.length) < 0) // linux kernel 2.6.33+
		{
			switch(errno)
			{
			case EAGAIN: //  Nonblocking I/O has been selected using O_NONBLOCK and the write would block.
			case EBADF: // The input file was not opened for reading or the output file was not opened for writing.
			case EFAULT: // Bad address.
			case EINVAL: // Descriptor is not valid or locked, or an mmap(2)-like operation is not available for in_fd, or count is negative.
			            // out_fd has the O_APPEND flag set.  This is not currently supported by sendfile().
			case EIO: //   Unspecified error while reading from in_fd.
			case ENOMEM: // Insufficient memory to read from in_fd.
			case EOVERFLOW: // count is too large, the operation would result in exceeding the maximum size of either the input file or the output file.
			case ESPIPE: // offset is not NULL but the input file is not seek(2)-able.
			default:
                break;
			}

			return eIOError;
		}
	}
	else if (NULL == segBufd.ptr)
		return eBadData;
	else if (write(fdFile, segBufd.ptr, fheader.data.length) < fheader.data.length)
		return eIOError;

/*
	printf("*******save() shmSegment file. key[%s]\n\n",  segBufd.key.c_str());
	testReadFile(segBufd.fd, segBufd.key);


	printf("\n\n*******save() disk cache file .key[%s]\n\n",  segBufd.key.c_str());
	testReadFile(fdFile, segBufd.key);
*/
	return eOK;
}

SegmentFile::IOError SegmentFile::save(const std::string& pathname, StreamSegment::BufDescriptor& segBufd)
{
	int fd = open(pathname.c_str(), O_RDWR | O_CREAT); // | O_TRUNC);
	if (fd <=0)
		return eIOError;

	IOError err = save(fd, segBufd);
	::close(fd);
	return err;
}

SegmentFile::IOError SegmentFile::loadHeader(int fdFile, StreamSegment::BufDescriptor& segBufd)
{
/*	// read the header by mmaping it
	Header* pHeader = (Header*)::mmap(NULL, sizeof(Header), PROT_READ, MAP_SHARED, fdFile, 0);

	if (MAP_FAILED == pHeader)
		return eNoHeader;
*/

	Header  header;
	int nread = read(fdFile, &header, sizeof(Header));
	if(nread != sizeof(Header))
	{
		if(nread < 0)
			return eNoHeader;
		else if(nread < sizeof(Header))
			return eBadHeader;
		else
			return eIOError;
	}
	IOError err = headerToBufDesc(header, segBufd);

//	::munmap(pHeader, sizeof(Header));

	if (eOK != err)
		return err;

	return eOK;
}

SegmentFile::IOError SegmentFile::load(const std::string& pathname, StreamSegment::BufDescriptor& segBufd,  ZQ::common::Log *pLog)
{
	FDWrapper::Ptr pFD = FDWrapper::openForLoad(pathname, pLog);
	/*int fd = open(pathname.c_str(), O_RDWR);*/
	if (!pFD || pFD->getFD() < 0)
		return eNotFound;

	int fd = pFD->getFD();

	IOError err =loadHeader(fd, segBufd);
	if (eOK != err)
	{
		//cautions:the class FDWrapper will close the fd in its distruct
		//close(fd);
		return err;
	}

	if(lseek(fd, 0, SEEK_SET) == -1)
	{
		err = eIOError;
		//cautions:the class FDWrapper will close the fd in its distruct
		//close(fd);
		return err;
	}

	do {
		err = eBadData;

		if (segBufd.fd >0) // 0 --> linux kernel 2.6.33+
		{
			 if(lseek(segBufd.fd, NULL, SEEK_SET) == -1)
			 {
				 err = eIOError;
				 break;
			 }

			ssize_t  nread = ::sendfile(segBufd.fd, fd, 0, sizeof(Header) + segBufd.length);

			if (nread <= 0)
			{
				switch(errno)
				{
				case EAGAIN: //  Nonblocking I/O has been selected using O_NONBLOCK and the write would block.
				case EBADF: // The input file was not opened for reading or the output file was not opened for writing.
				case EFAULT: // Bad address.
				case EINVAL: // Descriptor is not valid or locked, or an mmap(2)-like operation is not available for in_fd, or count is negative.
				             // out_fd has the O_APPEND flag set.  This is not currently supported by sendfile().
				case EIO: //   Unspecified error while reading from in_fd.
				case ENOMEM: // Insufficient memory to read from in_fd.
				case EOVERFLOW: // count is too large, the operation would result in exceeding the maximum size of either the input file or the output file.
				case ESPIPE: // offset is not NULL but the input file is not seek(2)-able.
				default:
                    break;
				}

				break; // do-while(0)
			}
		}
		else if (NULL == segBufd.ptr)
		{
			err = eBadBuff;
			break;
		}
		else if (lseek(fd, sizeof(Header), SEEK_SET) != sizeof(Header) || read(fd, segBufd.ptr, segBufd.length) < segBufd.length)
		{
			err = eBadData;
			break;
		}

		err = eOK;
	} while(0);
/*
    ///check data
	std::string filename = "/opt/TianShan/temp/" + segBufd.key + std::string(".dc");
	int npos = segBufd.key.find_last_of('/');
	if(npos > 0)
		filename = "/opt/TianShan/temp/" + segBufd.key.substr(npos +1)+ std::string(".dc");

	int fdw = open(filename.c_str(), O_RDWR | O_CREAT); // | O_TRUNC);
	if (fdw  > 0)
	{
		lseek(segBufd.fd, 0, SEEK_SET);

		ssize_t  nbytes = ::sendfile(fdw, segBufd.fd, NULL, sizeof(Header) + segBufd.length);
		::close(fdw);
	}


	printf("*******load() disk cache file. key[%s]\n\n",  segBufd.key.c_str());
    testReadFile(fd, segBufd.key);

	printf("\n\n*******load() disk cache file to shmSegment file. key[%s]\n\n",  segBufd.key.c_str());
	testReadFile(segBufd.fd, segBufd.key);
*/


	//cautions:the class FDWrapper will close the fd in its distruct
	//close(fd);
	return err;
}

#endif // ZQ_OS

}}//namespace
