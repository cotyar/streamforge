using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using StreamForge.Abstractions;

namespace StreamForge.Api.Auth;

/// <summary>Issues HS256 JWTs for authenticated users. 12h expiry.</summary>
public sealed class JwtTokenService(IConfiguration config)
{
    /// <summary>How long an issued token stays valid. Exposed so anything that has to expire in step
    /// with it — the documentation cookie in <see cref="DocsAuthCookie"/> — says 12h only once.</summary>
    public static readonly TimeSpan Lifetime = TimeSpan.FromHours(12);

    public string CreateToken(UserRecord user)
    {
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(config["Jwt:Key"]!));
        var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        var claims = new[]
        {
            new Claim(JwtRegisteredClaimNames.Sub, user.Username),
            new Claim(ClaimTypes.Name, user.Username),
            new Claim("name", user.DisplayName),
            new Claim(ClaimTypes.Role, user.Role),
            // One id per login — what ChatRateLimiter counts against, since the demo logins are
            // shared and the username alone would make one visitor's budget everyone's.
            new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString("N")),
        };

        var token = new JwtSecurityToken(
            issuer: config["Jwt:Issuer"],
            audience: config["Jwt:Audience"],
            claims: claims,
            expires: DateTime.UtcNow.Add(Lifetime),
            signingCredentials: creds);

        return new JwtSecurityTokenHandler().WriteToken(token);
    }
}
