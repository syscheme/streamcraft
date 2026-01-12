#ifndef __STREAMCRAFT_MemPage_H__
#define __STREAMCRAFT_MemPage_H__

#include "ZQ_common_conf.h"
#include "Pointer.h"
#include "Log.h"
#include "Locks.h"
#include "eloop.h"
#include "LRUMap.h"

#include <list>
namespace syscheme {
namespace StreamCraft {

#define INVALID_FD    (-1)
#ifndef MEMPAGE_BITLEN
#  define MEMPAGE_BITLEN  (21) // 2MB
#endif // MEMPAGE_BITLEN
#define MEMPAGE_SIZE  (1<<(MEMPAGE_BITLEN)) // 2MB
#define SEGMENT_KEYLEN  11   // do brace this value, 11hex=44bit, (44+21) is enough to hold a 64bit filesize in MEMPAGE_SIZE
#ifndef SEG_IDX_OF_OFFSET
#define SEG_IDX_OF_OFFSET(_OFFSET)  ((_OFFSET) >> MEMPAGE_BITLEN)
#endif
#define SEG_STARTOFFSET(_SEGIDX)  (((int64)_SEGIDX) <<MEMPAGE_BITLEN)
#define ROUND_DOWN_OFFSET(_OFFSET)  (SEG_STARTOFFSET(SEG_IDX_OF_OFFSET(_OFFSET)))

#ifndef FLAG
#  define FLAG(_BIT) (1 << _BIT)
#endif // FLAG

#ifndef TSPACKET_LENTH
#  define TSPACKET_LENTH   (188)
#endif // TSPACKET_LENTH

class SegmentManager;

// -----------------------------
// class StreamSegment
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
class StreamSegment : public ZQ::common::SharedObject
{
	friend class SegmentManager;
	friend class SegmentFile;

public:
	typedef ZQ::common::Pointer< StreamSegment > Ptr;
	typedef std::list<Ptr> Segments;
//	typedef std::queue< Ptr > Queue;

	typedef struct _BufDescriptor
	{
		std::string key;   // the unique key to identify the buf
		int    fd;         // the fd to the shm
		uint32 maxSize;    // the maximal bytes of pay-load data that is allowed to filled into the buffer, excluding the size of file header/footer if have any
		uint8* ptr;        // the memory address if valid
		uint32 length;     // the bytes of avaliable data start from offset=0 in the fd
		uint32 flags;      // the fd to the shm
		int64  stampAsOfOrigin;   // the timestamp as of when received from the orign

		_BufDescriptor() { fd = INVALID_FD; maxSize = 0; ptr = NULL; length = 0; flags = 0; }

	} BufDescriptor;

	typedef enum _StreamSegmentFlags
	{
		sfReadOnly,
		sfDirtyLeading,
	} StreamSegmentFlags;

protected:

	SegmentManager& _segm;
	BufDescriptor _bufd;
	int64         _offsetOfOrigin; // the offset of the source file that offset = 0 in the fd maps to
	int           _headLength;
	  

	// only hatched by the SegementManager
	StreamSegment(SegmentManager& sm, BufDescriptor& bufd, const std::string& uriOrigin, int64 offsetOrigin, int HeadLength = 0);

public:

	virtual ~StreamSegment();

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
	int getMaxSize() const { return _bufd.maxSize; }
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
// class SegmentManager
// -----------------------------
class SegmentManager
{
	friend class StreamSegment;

public:
	typedef std::list<StreamSegment::BufDescriptor> SegmentList;

public:
	SegmentManager(ZQ::common::Log& log, ZQ::eloop::Loop& loop, uint segPoolInit =0, uint segPoolMax = 4000);
	virtual ~SegmentManager() { clear(); }

	// allocate a segment with key as the purpsoe
	// the function is designed to support retries
	///@return pointer to the allocated segment if successful; NULL if fail
	///         - NULL when the SegmentManager is busy or waiting for file load of DiskCache, the caller can retry after an interval
	///         - NULL if out-of-memory after failure/timeout at DiskCache loading
	virtual StreamSegment::Ptr allocate(const std::string& uriOrigin, int64 offsetOrigin);

	// StreamSegment::Ptr allocateEmptyPacket(const std::string& uriOrigin, int64 offsetOrigin);
	// void freeEmptyPacket(StreamSegment::Ptr seg);
	StreamSegment::Ptr newNullSegment(const std::string& uriOrigin, int64 offsetOrigin);
	void freeNullSegment(StreamSegment::Ptr seg);

	virtual void free(StreamSegment::BufDescriptor bufd);
	virtual size_t freeSize() { return _segIdle.size(); }

	virtual void onFull(StreamSegment::Ptr seg);
	virtual void updateSegment(StreamSegment::Ptr seg);

	uint16	getVerbosity() { return (ZQ::common::Log::loglevel_t) _lw.getVerbosity() | (_verboseFlags<<8); }
	void    setVerbosity(uint16 verbose = (0 | ZQ::common::Log::L_ERROR)) { _lw.setVerbosity(verbose & 0x0f); _verboseFlags =verbose>>8; }

	void clear();

	//	ZQ::common::Log& _log;
protected:	
	virtual void free(StreamSegment::Ptr seg);

	template <class SegmentT>
	StreamSegment::Ptr hatchSegObj(StreamSegment::BufDescriptor& bufd, const std::string& uriOrigin = "default", int64 offsetOrigin = 0)
	{
		if(bufd.key.empty())
			bufd.key = SegmentManager::genShmkey(uriOrigin, offsetOrigin);

		StreamSegment::Ptr seg = new SegmentT(*this, bufd, uriOrigin, offsetOrigin);
		return seg;
	}

public:
	static  std::string genShmkey(const std::string& uriOrigin, int64 offsetOrigin);

private:
	SegmentList				_segPool;
	SegmentList				_segIdle;
	uint					_segPoolMax;

protected:
	ZQ::common::LogWrapper	_lw;
	uint16					_verboseFlags;
};

// -----------------------------
// class SegmentFile
// -----------------------------
// the desriptor of buffer, target will be the fd to the shm
#define HEADLEN_SegmentFile (512)
#define URL_MAX_LENGTH      (300)

class SegmentFile
{
public:
	typedef enum _IOError {
		eOK = 0, 
		eError  =100, 
		eBadBuff=400, eBadFD, eNotFound=404,
		eIOError=500, eNoHeader, eBadHeader, eBadURL, eBadData, eCRCError, eMissMatched, eQueueOverflow
	} IOError;

	typedef union _Header
	{
		uint8 bytes[HEADLEN_SegmentFile];
		struct
		{
			uint32 signature;
			uint16 version;
			uint32  length;
			uint32 flags;
			int64  stampAsOfOrigin;

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
	static IOError bufDescToHeader(const StreamSegment::BufDescriptor& segBufd, Header& fheader);

	//@note also cover fheader validation
	static IOError headerToBufDesc(Header& fheader, StreamSegment::BufDescriptor& segBufd);

	static IOError save(const std::string& pathname, StreamSegment::BufDescriptor& segBufd);
	static IOError load(const std::string& pathname, StreamSegment::BufDescriptor& segBufd,  ZQ::common::Log *pLog = NULL);

    static IOError loadHeader(int fdFile, StreamSegment::BufDescriptor& segBufd);

#ifndef ZQ_OS_MSWIN
	static IOError save(int fdFile, const StreamSegment::BufDescriptor& segBufd);
	static IOError load(int fdFile, StreamSegment::BufDescriptor& segBufd);

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

#endif // __STREAMCRAFT_MemPage_H__
