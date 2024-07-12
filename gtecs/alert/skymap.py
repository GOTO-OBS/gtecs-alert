"""Functions to handle event skymaps."""

from gzip import GzipFile
from io import BytesIO

from astropy.io import fits

from gototile.skymap import SkyMap


def skymap_from_bytes(bytes):
    """Create a SkyMap from an encoded FITS-format byte string."""
    # TODO: Should be a class method of `gototile.skymap.SkyMap`?
    try:
        hdu = fits.open(BytesIO(bytes))
    except OSError:
        # It might be compressed
        gzip = GzipFile(fileobj=BytesIO(bytes), mode='rb')
        hdu = fits.open(gzip)
    return SkyMap.from_fits(hdu)
