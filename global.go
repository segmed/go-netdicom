package netdicom

var allTransferSyntaxes = []string{
	"1.2.840.10008.1.2.1",     // Explicit VR Little Endian,
	"1.2.840.10008.1.2",       // Implicit VR Little Endian,
	"1.2.840.10008.1.2.1.99",  // Deflated Explicit VR Little Endian
	"1.2.840.10008.1.2.2",     // Explicit VR Big Endian,
	"1.2.840.10008.1.2.4.50",  // JPEG Baseline
	"1.2.840.10008.1.2.4.51",  // JPEG Extended
	"1.2.840.10008.1.2.4.57",  // JPEG Lossless P14
	"1.2.840.10008.1.2.4.70",  // JPEG Lossless
	"1.2.840.10008.1.2.4.80",  // JPEG-LS Lossless
	"1.2.840.10008.1.2.4.81",  // JPEG-LS Lossy
	"1.2.840.10008.1.2.4.90",  // JPEG 2000 Lossless
	"1.2.840.10008.1.2.4.91",  // JPEG 2000
	"1.2.840.10008.1.2.4.92",  // JPEG 2000 Multi-Component Lossless
	"1.2.840.10008.1.2.4.93",  // JPEG 2000 Multi-Component
	"1.2.840.10008.1.2.4.94",  // JPIP Referenced
	"1.2.840.10008.1.2.4.95",  // JPIP Referenced Deflate
	"1.2.840.10008.1.2.4.100", // MPEG2 Main Profile / Main Level
	"1.2.840.10008.1.2.4.101", // MPEG2 Main Profile / High Level
	"1.2.840.10008.1.2.4.102", // MPEG-4 AVC/H.264 High Profile / Level 4.1
	"1.2.840.10008.1.2.4.103", // MPEG-4 AVC/H.264 BD-compatible High Profile
	"1.2.840.10008.1.2.4.104", // MPEG-4 AVC/H.264 High Profile For 2D Video
	"1.2.840.10008.1.2.4.105", // MPEG-4 AVC/H.264 High Profile For 3D Video
	"1.2.840.10008.1.2.4.106", // MPEG-4 AVC/H.264 Stereo High Profile
	"1.2.840.10008.1.2.4.107", // HEVC/H.265 Main Profile / Level 5.1
	"1.2.840.10008.1.2.4.108", // HEVC/H.265 Main 10 Profile / Level 5.1
	"1.2.840.10008.1.2.5",     // RLE Lossless
}
