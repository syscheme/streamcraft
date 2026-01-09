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
// Ident : $Id: LargeContentPage.h $
// Branch: $Name:  $
// Author: Hui Shao
// Desc  : Define stream segment
//
// Revision History: 
// ---------------------------------------------------------------------------
// $Log: /ZQProjs/Common/Exception.h $
// ===========================================================================

#ifndef __streamcraft_PageFile_H__
#define __streamcraft_PageFile_H__

#include "ZQ_common_conf.h"
#include "LargeContentPage.h"

#include <list>
namespace syscheme {
namespace StreamCraft {

// -----------------------------
// class PageFile
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
#define HEADLEN_SegmentFile (512)
#define URL_MAX_LENGTH      (300)

class PageFile
{
public:
	typedef enum _IOError {
		eOK     = 0, 
		eError  =100, 
		eBadBuff=400, eBadFD, eNotFound=404,
		eIOError=500, eNoHeader, eBadHeader, eBadURL, eBadData, eCRCError, eMissMatched, eQueueOverflow
	} IOError;

	typedef union _Header
	{
		uint8 bytes[HEADLEN_SegmentFile];
		struct
		{
			uint32  signature;
			uint16  version;
			uint32  length;
			uint32  flags;
			int64   stampAsOfOrigin;     // the timestamp as of when received from the orign urlKey

			uint8  reserved[78]; // to No.100Bytes
			char   urlKey[URL_MAX_LENGTH];
		} data;

		_Header()
		{
		  memset(bytes, 0, HEADLEN_SegmentFile);
		  data.signature = 0; 
		  data.version = 0;
		  data.length = 0;
		  data.flags = 0;
		  data.stampAsOfOrigin =0;
		  memset(data.reserved, 0, 78);
		  memset(data.urlKey, 0, URL_MAX_LENGTH);
		}
	} Header;

	//@note also cover fheader fixup
	static IOError bufDescToHeader(const LargeContentPage::PageDescriptor& segBufd, Header& fheader);

	//@note also cover fheader validation
	static IOError headerToBufDesc(Header& fheader, LargeContentPage::PageDescriptor& segBufd);

	static IOError save(const std::string& pathname, LargeContentPage::PageDescriptor& segBufd);
	static IOError load(const std::string& pathname, LargeContentPage::PageDescriptor& segBufd,  ZQ::common::Log *pLog = NULL);

    static IOError loadHeader(int fdFile, LargeContentPage::PageDescriptor& segBufd);

#ifndef ZQ_OS_MSWIN
	static IOError save(int fdFile, const LargeContentPage::PageDescriptor& segBufd);
	static IOError load(int fdFile, LargeContentPage::PageDescriptor& segBufd);

#endif // ZQ_OS
};

// -----------------------------
// class FDWrapper
// -----------------------------
//used for store fd opened previously
class FDWrapper: public ZQ::common::SharedObject
{
public:
	typedef ZQ::common::Pointer<FDWrapper> Ptr;
	typedef ZQ::common::LRUMap<std::string, Ptr> Map;
	FDWrapper(int fd) : _fd(fd) {}
	virtual ~FDWrapper() { if (_fd >=0) ::close(_fd); }

	static FDWrapper::Ptr openForLoad(const std::string& pathname, ZQ::common::Log *pLog = NULL);
	static bool preOpenForLoad(const std::string& pathname,  ZQ::common::Log *pLog = NULL);

	int getFD(){return _fd;}

protected:
	virtual bool isVaildFD(){if(_fd >= 0)return true;else return false;}

	int _fd;
};

}}//namespace

#endif // __streamcraft_PageFile_H__
